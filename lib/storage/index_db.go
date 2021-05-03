package storage

import (
	"bytes"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/workingsetcache"
	"github.com/VictoriaMetrics/fastcache"
	"github.com/faceair/VictoriaTraces/lib/uint128"
)

const (
	// Prefix for check metric id exist.
	nsPrefixMetricIDToMetricName = 0
	nsPrefixMetricNameToMetricID = 1

	// Prefix for (Tag,Time)->TraceID entries.
	nsPrefixTagKeyValue      = 2
	nsPrefixTagTimeToTraceID = 3
)

func shouldCacheBlock(item []byte) bool {
	if len(item) == 0 {
		return true
	}
	// Do not cache items starting from
	switch item[0] {
	case 1:
		// Do not cache blocks with tag->metricIDs and (date,tag)->metricIDs items, since:
		// - these blocks are scanned sequentially, so the overhead
		//   on their unmarshaling is amortized by the sequential scan.
		// - these blocks can occupy high amounts of RAM in cache
		//   and evict other frequently accessed blocks.
		return false
	default:
		return true
	}
}

// indexDB represents an index db.
type indexDB struct {
	// Atomic counters must go at the top of the structure in order to properly align by 8 bytes on 32-bit archs.
	// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/212 .

	refCount uint64

	// The counter for newly created time series. It can be used for determining time series churn rate.
	newTimeseriesCreated uint64

	// The number of missing MetricID -> TraceID entries.
	// hi rate for this value means corrupted indexDB.
	missingTSIDsForMetricID uint64

	// The number of searches for metric ids by days.
	dateMetricIDsSearchCalls uint64

	// The number of successful searches for metric ids by days.
	dateMetricIDsSearchHits uint64

	// The number of calls for date range searches.
	dateRangeSearchCalls uint64

	// The number of hits for date range searches.
	dateRangeSearchHits uint64

	// missingMetricNamesForMetricID is a counter of missing MetricID -> SpanName entries.
	// hi rate may mean corrupted indexDB due to unclean shutdown.
	// The db must be automatically recovered after that.
	missingMetricNamesForMetricID uint64

	mustDrop uint64

	// Start date fully covered by per-day inverted index.
	startDateForPerDayInvertedIndex uint64

	name string
	tb   *mergeset.Table

	extDB     *indexDB
	extDBLock sync.Mutex

	// Cache for fast TagFilters -> TSIDs lookup.
	tagCache *workingsetcache.Cache

	// Cache for fast MetricID -> SpanName lookup.
	metricNameCache *workingsetcache.Cache
	metricIDCache   *workingsetcache.Cache

	// Cache for useless TagFilters entries, which have no tag filters
	// matching low number of metrics.
	uselessTagFiltersCache *workingsetcache.Cache

	// Cache for (date, tagFilter) -> metricIDsLen, which is used for reducing
	// the amount of work when matching a set of filters.
	traceIDsPerDateTagFilterCache *workingsetcache.Cache

	indexSearchPool sync.Pool
}

// openIndexDB opens index db from the given path with the given caches.
func openIndexDB(path string, metricIDCache, metricNameCache *workingsetcache.Cache) (*indexDB, error) {
	tb, err := mergeset.OpenTable(path, func() {}, mergeTagToTraceIDsRows)
	if err != nil {
		return nil, fmt.Errorf("cannot open indexDB %q: %w", path, err)
	}

	name := filepath.Base(path)

	// Do not persist tagCache in files, since it is very volatile.
	mem := memory.Allowed()

	db := &indexDB{
		refCount:                      1,
		tb:                            tb,
		name:                          name,
		tagCache:                      workingsetcache.New(mem/32, time.Hour),
		metricIDCache:                 metricIDCache,
		metricNameCache:               metricNameCache,
		uselessTagFiltersCache:        workingsetcache.New(mem/128, time.Hour),
		traceIDsPerDateTagFilterCache: workingsetcache.New(mem/128, time.Hour),
	}
	return db, nil
}

const noDeadline = 1<<64 - 1

// IndexDBMetrics contains essential metrics for indexDB.
type IndexDBMetrics struct {
	TagCacheSize      uint64
	TagCacheSizeBytes uint64
	TagCacheRequests  uint64
	TagCacheMisses    uint64

	UselessTagFiltersCacheSize      uint64
	UselessTagFiltersCacheSizeBytes uint64
	UselessTagFiltersCacheRequests  uint64
	UselessTagFiltersCacheMisses    uint64

	IndexDBRefCount uint64

	NewTimeseriesCreated    uint64
	MissingTSIDsForMetricID uint64

	RecentHourMetricIDsSearchCalls uint64
	RecentHourMetricIDsSearchHits  uint64
	DateMetricIDsSearchCalls       uint64
	DateMetricIDsSearchHits        uint64

	DateRangeSearchCalls uint64
	DateRangeSearchHits  uint64

	MissingMetricNamesForMetricID uint64

	IndexBlocksWithMetricIDsProcessed      uint64
	IndexBlocksWithMetricIDsIncorrectOrder uint64

	mergeset.TableMetrics
}

func (db *indexDB) scheduleToDrop() {
	atomic.AddUint64(&db.mustDrop, 1)
}

// UpdateMetrics updates m with metrics from the db.
func (db *indexDB) UpdateMetrics(m *IndexDBMetrics) {
	var cs fastcache.Stats

	cs.Reset()
	db.tagCache.UpdateStats(&cs)
	m.TagCacheSize += cs.EntriesCount
	m.TagCacheSizeBytes += cs.BytesSize
	m.TagCacheRequests += cs.GetBigCalls
	m.TagCacheMisses += cs.Misses

	cs.Reset()
	db.uselessTagFiltersCache.UpdateStats(&cs)
	m.UselessTagFiltersCacheSize += cs.EntriesCount
	m.UselessTagFiltersCacheSizeBytes += cs.BytesSize
	m.UselessTagFiltersCacheRequests += cs.GetCalls
	m.UselessTagFiltersCacheMisses += cs.Misses

	m.IndexDBRefCount += atomic.LoadUint64(&db.refCount)
	m.NewTimeseriesCreated += atomic.LoadUint64(&db.newTimeseriesCreated)
	m.MissingTSIDsForMetricID += atomic.LoadUint64(&db.missingTSIDsForMetricID)
	m.DateMetricIDsSearchCalls += atomic.LoadUint64(&db.dateMetricIDsSearchCalls)
	m.DateMetricIDsSearchHits += atomic.LoadUint64(&db.dateMetricIDsSearchHits)

	m.DateRangeSearchCalls += atomic.LoadUint64(&db.dateRangeSearchCalls)
	m.DateRangeSearchHits += atomic.LoadUint64(&db.dateRangeSearchHits)

	m.MissingMetricNamesForMetricID += atomic.LoadUint64(&db.missingMetricNamesForMetricID)

	m.IndexBlocksWithMetricIDsProcessed = atomic.LoadUint64(&indexBlocksWithTraceIDsProcessed)
	m.IndexBlocksWithMetricIDsIncorrectOrder = atomic.LoadUint64(&indexBlocksWithTraceIDsIncorrectOrder)

	db.tb.UpdateMetrics(&m.TableMetrics)
	db.doExtDB(func(extDB *indexDB) {
		extDB.tb.UpdateMetrics(&m.TableMetrics)
		m.IndexDBRefCount += atomic.LoadUint64(&extDB.refCount)
	})
}

func (db *indexDB) doExtDB(f func(extDB *indexDB)) bool {
	db.extDBLock.Lock()
	extDB := db.extDB
	if extDB != nil {
		extDB.incRef()
	}
	db.extDBLock.Unlock()
	if extDB == nil {
		return false
	}
	f(extDB)
	extDB.decRef()
	return true
}

// SetExtDB sets external db to search.
//
// It decrements refCount for the previous extDB.
func (db *indexDB) SetExtDB(extDB *indexDB) {
	db.extDBLock.Lock()
	prevExtDB := db.extDB
	db.extDB = extDB
	db.extDBLock.Unlock()

	if prevExtDB != nil {
		prevExtDB.decRef()
	}
}

// MustClose closes db.
func (db *indexDB) MustClose() {
	db.decRef()
}

func (db *indexDB) incRef() {
	atomic.AddUint64(&db.refCount, 1)
}

func (db *indexDB) decRef() {
	n := atomic.AddUint64(&db.refCount, ^uint64(0))
	if int64(n) < 0 {
		logger.Panicf("BUG: negative refCount: %d", n)
	}
	if n > 0 {
		return
	}

	tbPath := db.tb.Path()
	db.tb.MustClose()
	db.SetExtDB(nil)

	// Free space occupied by caches owned by db.
	db.tagCache.Stop()
	db.uselessTagFiltersCache.Stop()
	db.traceIDsPerDateTagFilterCache.Stop()

	db.tagCache = nil
	db.metricNameCache = nil
	db.uselessTagFiltersCache = nil
	db.traceIDsPerDateTagFilterCache = nil

	if atomic.LoadUint64(&db.mustDrop) == 0 {
		return
	}

	logger.Infof("dropping indexDB %q", tbPath)
	fs.MustRemoveAll(tbPath)
	logger.Infof("indexDB %q has been dropped", tbPath)
}

func (db *indexDB) getMetricNameFromCache(metricName []byte, metricID uint64) []byte {
	key := (*[unsafe.Sizeof(metricID)]byte)(unsafe.Pointer(&metricID))
	return db.metricNameCache.Get(metricName[:0], key[:])
}

func (db *indexDB) getMetricIDFromCache(metricName []byte) (uint64, error) {
	metricID := uint64(0)
	id := (*[unsafe.Sizeof(metricID)]byte)(unsafe.Pointer(&metricID))
	tmp := db.metricIDCache.Get(id[:0], metricName[:])
	if len(tmp) == 0 || metricID == 0 {
		// The TraceID for the given metricID wasn't found in the cache.
		return 0, io.EOF
	}
	return metricID, nil
}

func (db *indexDB) putMetricToCache(metricID uint64, metricName []byte) {
	id := (*[unsafe.Sizeof(metricID)]byte)(unsafe.Pointer(&metricID))
	db.metricNameCache.Set(id[:], metricName[:])
	db.metricIDCache.Set(metricName[:], id[:])
}

type indexSearch struct {
	db *indexDB
	ts mergeset.TableSearch
	kb bytesutil.ByteBuffer
	mp tagToTraceIDsRowParser

	// deadline in unix timestamp seconds for the given search.
	deadline uint64

	// tsidByNameMisses and tsidByNameSkips is used for a performance
	// hack in GetOrCreateTSIDByName. See the comment there.
	tsidByNameMisses int
	tsidByNameSkips  int
}

func (db *indexDB) getIndexSearch(deadline uint64) *indexSearch {
	v := db.indexSearchPool.Get()
	if v == nil {
		v = &indexSearch{
			db: db,
		}
	}
	is := v.(*indexSearch)
	is.ts.Init(db.tb, shouldCacheBlock)
	is.deadline = deadline
	return is
}

func (db *indexDB) putIndexSearch(is *indexSearch) {
	is.ts.MustClose()
	is.kb.Reset()
	is.mp.Reset()
	is.deadline = 0

	// Do not reset tsidByNameMisses and tsidByNameSkips,
	// since they are used in GetOrCreateTSIDByName across call boundaries.
	db.indexSearchPool.Put(is)
}

func (db *indexDB) createMetricIDByName(metricName []byte) (uint64, error) {
	metricID, err := db.generateMetricID(metricName)
	if err != nil {
		return 0, fmt.Errorf("cannot generate MetricID: %w", err)
	}

	err = db.createMetricIndex(metricID, metricName)
	if err != nil {
		return 0, fmt.Errorf("cannot create indexes: %w", err)
	}

	// There is no need in invalidating tag cache, since it is invalidated
	// on db.tb flush via invalidateTagCache flushCallback passed to OpenTable.
	db.putMetricToCache(metricID, metricName)
	atomic.AddUint64(&db.newTimeseriesCreated, 1)
	return metricID, nil
}

func (db *indexDB) generateMetricID(metricName []byte) (uint64, error) {
	// Search the TraceID in the external storage.
	// This is usually the db from the previous period.
	var err error
	var metricID uint64
	if db.doExtDB(func(extDB *indexDB) {
		metricID, err = extDB.getMetricIDByMetricName(metricName)
	}) {
		if err == nil {
			// The TraceID has been found in the external storage.
			return metricID, nil
		}
		if err != io.EOF {
			return 0, fmt.Errorf("external search failed: %w", err)
		}
	}
	metricID = generateUniqueMetricID()
	return metricID, nil
}

// getTSIDByNameNoCreate fills the dst with TraceID for the given metricName.
//
// It returns io.EOF if the given mn isn't found locally.
func (db *indexDB) getMetricIDByMetricName(metricName []byte) (uint64, error) {
	is := db.getIndexSearch(noDeadline)
	metricID, err := is.getMetricIDByMetricName(metricName)
	db.putIndexSearch(is)
	if err == nil {
		return metricID, nil
	}
	if err != io.EOF {
		return 0, fmt.Errorf("cannot search TraceID by MetricName %q: %w", metricName, err)
	}

	// Do not search for the TraceID in the external storage,
	// since this function is already called by another indexDB instance.

	// The TraceID for the given mn wasn't found.
	return 0, io.EOF
}

func (db *indexDB) createMetricIndex(metricID uint64, metricName []byte) error {
	items := getIndexItems()
	defer putIndexItems(items)

	mn := GetSpanName()
	defer PutSpanName(mn)

	if err := mn.unmarshalRaw(metricName); err != nil {
		return fmt.Errorf("cannot unmarshal metricName %q obtained by metricID %d: %w", metricName, metricID, err)
	}

	items.B = marshalCommonPrefix(items.B, nsPrefixMetricIDToMetricName)
	items.B = encoding.MarshalUint64(items.B, metricID)
	items.B = append(items.B, kvSeparatorChar)
	items.B = append(items.B, metricName...)
	items.Next()

	items.B = marshalCommonPrefix(items.B, nsPrefixMetricNameToMetricID)
	items.B = append(items.B, metricName...)
	items.B = append(items.B, kvSeparatorChar)
	items.B = encoding.MarshalUint64(items.B, metricID)
	items.Next()

	for i := range mn.Tags {
		tag := &mn.Tags[i]
		items.B = marshalCommonPrefix(items.B, nsPrefixTagKeyValue)
		items.B = tag.Marshal(items.B)
		items.Next()
	}

	return db.tb.AddItems(items.Items)
}

type indexItems struct {
	B     []byte
	Items [][]byte

	start int
}

func (ii *indexItems) reset() {
	ii.B = ii.B[:0]
	ii.Items = ii.Items[:0]
	ii.start = 0
}

func (ii *indexItems) Next() {
	ii.Items = append(ii.Items, ii.B[ii.start:])
	ii.start = len(ii.B)
}

func getIndexItems() *indexItems {
	v := indexItemsPool.Get()
	if v == nil {
		return &indexItems{}
	}
	return v.(*indexItems)
}

func putIndexItems(ii *indexItems) {
	ii.reset()
	indexItemsPool.Put(ii)
}

var indexItemsPool sync.Pool

// SearchTagKeys returns all the tag keys.
func (db *indexDB) SearchTagKeys(maxTagKeys int, deadline uint64) ([]string, error) {
	// TODO: cache results?

	tks := make(map[string]struct{})

	is := db.getIndexSearch(deadline)
	err := is.searchTagKeys(tks, maxTagKeys)
	db.putIndexSearch(is)
	if err != nil {
		return nil, err
	}

	ok := db.doExtDB(func(extDB *indexDB) {
		is := extDB.getIndexSearch(deadline)
		err = is.searchTagKeys(tks, maxTagKeys)
		extDB.putIndexSearch(is)
	})
	if ok && err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(tks))
	for key := range tks {
		// Do not skip empty keys, since they are converted to __name__
		keys = append(keys, key)
	}

	// Do not sort keys, since they must be sorted by vmselect.
	return keys, nil
}

func (is *indexSearch) searchTagKeys(tks map[string]struct{}, maxTagKeys int) error {
	ts := &is.ts
	kb := &is.kb
	mp := &is.mp
	mp.Reset()
	loopsPaceLimiter := 0
	kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefixTagKeyValue)
	prefix := kb.B
	ts.Seek(prefix)
	for len(tks) < maxTagKeys && ts.NextItem() {
		if loopsPaceLimiter&paceLimiterFastIterationsMask == 0 {
			if err := checkSearchDeadlineAndPace(is.deadline); err != nil {
				return err
			}
		}
		loopsPaceLimiter++
		item := ts.Item
		if !bytes.HasPrefix(item, prefix) {
			break
		}
		if err := mp.Init(item, nsPrefixTagKeyValue); err != nil {
			return err
		}

		// Store tag key.
		tks[string(mp.Tag.Key)] = struct{}{}

		// Search for the next tag key.
		// The last char in kb.B must be tagSeparatorChar.
		// Just increment it in order to jump to the next tag key.
		kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefixTagKeyValue)
		kb.B = marshalTagValue(kb.B, mp.Tag.Key)
		kb.B[len(kb.B)-1]++
		ts.Seek(kb.B)
	}
	if err := ts.Error(); err != nil {
		return fmt.Errorf("error during search for prefix %q: %w", prefix, err)
	}
	return nil
}

// SearchTagValues returns all the tag values for the given tagKey
func (db *indexDB) SearchTagValues(tagKey []byte, maxTagValues int, deadline uint64) ([]string, error) {
	// TODO: cache results?

	tvs := make(map[string]struct{})
	is := db.getIndexSearch(deadline)
	err := is.searchTagValues(tvs, tagKey, maxTagValues)
	db.putIndexSearch(is)
	if err != nil {
		return nil, err
	}
	ok := db.doExtDB(func(extDB *indexDB) {
		is := extDB.getIndexSearch(deadline)
		err = is.searchTagValues(tvs, tagKey, maxTagValues)
		extDB.putIndexSearch(is)
	})
	if ok && err != nil {
		return nil, err
	}

	tagValues := make([]string, 0, len(tvs))
	for tv := range tvs {
		if len(tv) == 0 {
			// Skip empty values, since they have no any meaning.
			// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/600
			continue
		}
		tagValues = append(tagValues, tv)
	}

	// Do not sort tagValues, since they must be sorted by vmselect.
	return tagValues, nil
}

// searchTraceIDs returns sorted tsids matching the given tfss over the given tr.
func (db *indexDB) searchTraceIDs(tfss []*TagFilters, tr ScanRange, deadline uint64) ([]TraceID, error) {
	if len(tfss) == 0 {
		return nil, nil
	}

	// Slow path - search for tsids in the db and extDB.
	is := db.getIndexSearch(deadline)
	localTSIDs, err := is.searchTraceIDs(tfss, tr)
	db.putIndexSearch(is)
	if err != nil {
		return nil, err
	}

	var extTSIDs []TraceID
	if db.doExtDB(func(extDB *indexDB) {

		is := extDB.getIndexSearch(deadline)
		extTSIDs, err = is.searchTraceIDs(tfss, tr)
		extDB.putIndexSearch(is)

		sort.Slice(extTSIDs, func(i, j int) bool { return extTSIDs[i].Less(extTSIDs[j]) })
	}) {
		if err != nil {
			return nil, err
		}
	}
	// Merge localTSIDs with extTSIDs.
	tsids := mergeTSIDs(localTSIDs, extTSIDs)

	// Sort the found tsids, since they must be passed to TraceID search
	// in the sorted order.
	sort.Slice(tsids, func(i, j int) bool { return tsids[i].Less(tsids[j]) })

	return tsids, err
}

func mergeTSIDs(a, b []TraceID) []TraceID {
	if len(b) > len(a) {
		a, b = b, a
	}
	if len(b) == 0 {
		return a
	}
	m := make(map[TraceID]TraceID, len(a))
	for i := range a {
		tsid := a[i]
		m[tsid] = tsid
	}
	for i := range b {
		tsid := b[i]
		m[tsid] = tsid
	}

	tsids := make([]TraceID, 0, len(m))
	for _, tsid := range m {
		tsids = append(tsids, tsid)
	}
	return tsids
}

func matchTagFilters(mn *SpanName, tfs []*tagFilter, kb *bytesutil.ByteBuffer) (bool, error) {
	kb.B = marshalCommonPrefix(kb.B[:0], nsPrefixTagTimeToTraceID)

	for i, tf := range tfs {
		if len(tf.key) == 0 {
			// Match against mn.TraceID.
			b := marshalTagValue(kb.B, nil)
			b = marshalTagValue(b, mn.TraceID)
			kb.B = b[:len(kb.B)]
			ok, err := matchTagFilter(b, tf)
			if err != nil {
				return false, fmt.Errorf("cannot match TraceID %q with tagFilter %s: %w", mn.TraceID, tf, err)
			}
			if !ok {
				// Move failed tf to start.
				// This should reduce the amount of useless work for the next mn.
				if i > 0 {
					tfs[0], tfs[i] = tfs[i], tfs[0]
				}
				return false, nil
			}
			continue
		}
		if bytes.Equal(tf.key, graphiteReverseTagKey) {
			// Skip artificial tag filter for Graphite-like metric names with dots,
			// since mn doesn't contain the corresponding tag.
			continue
		}

		// Search for matching tag name.
		tagMatched := false
		tagSeen := false
		for j := range mn.Tags {
			tag := &mn.Tags[j]
			if string(tag.Key) != string(tf.key) {
				continue
			}

			// Found the matching tag name. Match the value.
			tagSeen = true
			b := tag.Marshal(kb.B)
			kb.B = b[:len(kb.B)]
			ok, err := matchTagFilter(b, tf)
			if err != nil {
				return false, fmt.Errorf("cannot match tag %q with tagFilter %s: %w", tag, tf, err)
			}
			if !ok {
				// Move failed tf to start.
				// This should reduce the amount of useless work for the next mn.
				if i > 0 {
					tfs[0], tfs[i] = tfs[i], tfs[0]
				}
				return false, nil
			}
			tagMatched = true
			break
		}
		if !tagSeen && tf.isNegative && !tf.isEmptyMatch {
			// tf contains negative filter for non-exsisting tag key
			// and this filter doesn't match empty string, i.e. {non_existing_tag_key!="foobar"}
			// Such filter matches anything.
			//
			// Note that the filter `{non_existing_tag_key!~"|foobar"}` shouldn't match anything,
			// since it is expected that it matches non-empty `non_existing_tag_key`.
			// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/546 for details.
			continue
		}
		if tagMatched {
			// tf matches mn. Go to the next tf.
			continue
		}
		// Matching tag name wasn't found.
		// Move failed tf to start.
		// This should reduce the amount of useless work for the next mn.
		if i > 0 {
			tfs[0], tfs[i] = tfs[i], tfs[0]
		}
		return false, nil
	}
	return true, nil
}

func matchTagFilter(b []byte, tf *tagFilter) (bool, error) {
	if !bytes.HasPrefix(b, tf.prefix) {
		return tf.isNegative, nil
	}
	ok, err := tf.matchSuffix(b[len(tf.prefix):])
	if err != nil {
		return false, err
	}
	if !ok {
		return tf.isNegative, nil
	}
	return !tf.isNegative, nil
}

// The tag key for reverse metric name used for speeding up searching
// for Graphite wildcards with suffix matching small number of time series,
// i.e. '*.bar.baz'.
//
// It is expected that the given key isn't be used by users.
var graphiteReverseTagKey = []byte("\xff")

func reverseBytes(dst, src []byte) []byte {
	for i := len(src) - 1; i >= 0; i-- {
		dst = append(dst, src[i])
	}
	return dst
}

func appendDateTagFilterCacheKey(dst []byte, date uint64, tf *tagFilter) []byte {
	dst = encoding.MarshalUint64(dst, date)
	dst = tf.Marshal(dst)
	return dst
}

var kbPool bytesutil.ByteBufferPool

// Returns local unique MetricID.
func generateUniqueMetricID() uint64 {
	// It is expected that metricIDs returned from this function must be dense.
	// If they will be sparse, then this may hurt metric_ids intersection
	// performance with uint64set.Set.
	return atomic.AddUint64(&nextUniqueMetricID, 1)
}

// This number mustn't go backwards on restarts, otherwise metricID
// collisions are possible. So don't change time on the server
// between VictoriaMetrics restarts.
var nextUniqueMetricID = uint64(time.Now().UnixNano())

func marshalCommonPrefix(dst []byte, nsPrefix byte) []byte {
	dst = append(dst, nsPrefix)
	return dst
}

func (is *indexSearch) marshalCommonPrefix(dst []byte, nsPrefix byte) []byte {
	return marshalCommonPrefix(dst, nsPrefix)
}

func unmarshalCommonPrefix(src []byte) ([]byte, byte, error) {
	if len(src) < commonPrefixLen {
		return nil, 0, fmt.Errorf("cannot unmarshal common prefix from %d bytes; need at least %d bytes; data=%X", len(src), commonPrefixLen, src)
	}
	prefix := src[0]
	return src[commonPrefixLen:], prefix, nil
}

// 1 byte for prefix
const commonPrefixLen = 1

type tagToTraceIDsRowParser struct {
	// NSPrefix contains the first byte parsed from the row after Init call.
	// This is either nsPrefixTagToMetricIDs or nsPrefixDateTagToMetricIDs.
	NSPrefix byte

	// TraceIDs contains parsed TraceIDs after ParseTraceIDs call
	TraceIDs []TraceID

	// Tag contains parsed tag after Init call
	Tag Tag

	// tail contains the remaining unparsed metricIDs
	tail []byte
}

func (mp *tagToTraceIDsRowParser) Reset() {
	mp.NSPrefix = 0
	mp.TraceIDs = mp.TraceIDs[:0]
	mp.Tag.Reset()
	mp.tail = nil
}

// Init initializes mp from b, which should contain encoded tag->metricIDs row.
//
// b cannot be re-used until Reset call.
func (mp *tagToTraceIDsRowParser) Init(b []byte, nsPrefixExpected byte) error {
	tail, nsPrefix, err := unmarshalCommonPrefix(b)
	if err != nil {
		return fmt.Errorf("invalid tag->metricIDs row %q: %w", b, err)
	}
	if nsPrefix != nsPrefixExpected {
		return fmt.Errorf("invalid prefix for tag->metricIDs row %q; got %d; want %d", b, nsPrefix, nsPrefixExpected)
	}
	mp.NSPrefix = nsPrefix
	tail, err = mp.Tag.Unmarshal(tail)
	if err != nil {
		return fmt.Errorf("cannot unmarshal tag from tag->metricIDs row %q: %w", b, err)
	}
	return mp.InitOnlyTail(b, tail)
}

// MarshalPrefix marshals row prefix without tail to dst.
func (mp *tagToTraceIDsRowParser) MarshalPrefix(dst []byte) []byte {
	dst = marshalCommonPrefix(dst, mp.NSPrefix)
	dst = mp.Tag.Marshal(dst)
	return dst
}

// InitOnlyTail initializes mp.tail from tail.
//
// b must contain tag->metricIDs row.
// b cannot be re-used until Reset call.
func (mp *tagToTraceIDsRowParser) InitOnlyTail(b, tail []byte) error {
	if len(tail) == 0 {
		return fmt.Errorf("missing metricID in the tag->metricIDs row %q", b)
	}
	if len(tail)%8 != 0 {
		return fmt.Errorf("invalid tail length in the tag->metricIDs row; got %d bytes; must be multiple of 8 bytes", len(tail))
	}
	mp.tail = tail
	return nil
}

// EqualPrefix returns true if prefixes for mp and x are equal.
//
// Prefix contains (tag)
func (mp *tagToTraceIDsRowParser) EqualPrefix(x *tagToTraceIDsRowParser) bool {
	if !mp.Tag.Equal(&x.Tag) {
		return false
	}
	return mp.NSPrefix == x.NSPrefix
}

// TraceIDsLen returns the number of TraceIDs in the mp.tail
func (mp *tagToTraceIDsRowParser) TraceIDsLen() int {
	return len(mp.tail) / 16
}

// ParseTraceIDs parses TraceIDs from mp.tail into mp.TraceIDs.
func (mp *tagToTraceIDsRowParser) ParseTraceIDs() {
	tail := mp.tail
	mp.TraceIDs = mp.TraceIDs[:0]
	n := len(tail) / 8
	if n <= cap(mp.TraceIDs) {
		mp.TraceIDs = mp.TraceIDs[:n]
	} else {
		mp.TraceIDs = append(mp.TraceIDs[:cap(mp.TraceIDs)], make([]TraceID, n-cap(mp.TraceIDs))...)
	}
	traceIDs := mp.TraceIDs
	_ = traceIDs[n-1]
	for i := 0; i < n; i++ {
		if len(tail) < 8 {
			logger.Panicf("BUG: tail cannot be smaller than 8 bytes; got %d bytes; tail=%X", len(tail), tail)
			return
		}

		traceID, tail, err := uint128.Unmarshal(tail)
		if err != nil {
			logger.Panicf("BUG: tail cannot unmarshal traceid; got %d bytes; tail=%X", len(tail), tail)
			return
		}
		traceIDs[i] = traceID
	}
}

// HasCommonTraceIDs returns true if mp has at least one common metric id with filter.
func (mp *tagToTraceIDsRowParser) HasCommonTraceIDs(filter *uint128.Set) bool {
	for _, traceID := range mp.TraceIDs {
		if filter.Has(traceID) {
			return true
		}
	}
	return false
}

func mergeTagToTraceIDsRows(data []byte, items [][]byte) ([]byte, [][]byte) {
	data, items = mergeTagToTraceIDsRowsInternal(data, items, nsPrefixTagTimeToTraceID)
	return data, items
}

func mergeTagToTraceIDsRowsInternal(data []byte, items [][]byte, nsPrefix byte) ([]byte, [][]byte) {
	// Perform quick checks whether items contain rows starting from nsPrefix
	// based on the fact that items are sorted.
	if len(items) <= 2 {
		// The first and the last row must remain unchanged.
		return data, items
	}
	firstItem := items[0]
	if len(firstItem) > 0 && firstItem[0] > nsPrefix {
		return data, items
	}
	lastItem := items[len(items)-1]
	if len(lastItem) > 0 && lastItem[0] < nsPrefix {
		return data, items
	}

	// items contain at least one row starting from nsPrefix. Merge rows with common tag.
	tmm := getTagToTraceIDsRowsMerger()
	tmm.dataCopy = append(tmm.dataCopy[:0], data...)
	tmm.itemsCopy = append(tmm.itemsCopy[:0], items...)
	mp := &tmm.mp
	mpPrev := &tmm.mpPrev
	dstData := data[:0]
	dstItems := items[:0]
	for i, item := range items {
		if len(item) == 0 || item[0] != nsPrefix || i == 0 || i == len(items)-1 {
			// Write rows not starting with nsPrefix as-is.
			// Additionally write the first and the last row as-is in order to preserve
			// sort order for adjancent blocks.
			dstData, dstItems = tmm.flushPendingTraceIDs(dstData, dstItems, mpPrev)
			dstData = append(dstData, item...)
			dstItems = append(dstItems, dstData[len(dstData)-len(item):])
			continue
		}
		if err := mp.Init(item, nsPrefix); err != nil {
			logger.Panicf("FATAL: cannot parse row starting with nsPrefix %d during merge: %s", nsPrefix, err)
		}
		if mp.TraceIDsLen() >= maxTraceIDsPerRow {
			dstData, dstItems = tmm.flushPendingTraceIDs(dstData, dstItems, mpPrev)
			dstData = append(dstData, item...)
			dstItems = append(dstItems, dstData[len(dstData)-len(item):])
			continue
		}
		if !mp.EqualPrefix(mpPrev) {
			dstData, dstItems = tmm.flushPendingTraceIDs(dstData, dstItems, mpPrev)
		}
		mp.ParseTraceIDs()
		tmm.pendingTraceIDs = append(tmm.pendingTraceIDs, mp.TraceIDs...)
		mpPrev, mp = mp, mpPrev
		if len(tmm.pendingTraceIDs) >= maxTraceIDsPerRow {
			dstData, dstItems = tmm.flushPendingTraceIDs(dstData, dstItems, mpPrev)
		}
	}
	if len(tmm.pendingTraceIDs) > 0 {
		logger.Panicf("BUG: tmm.pendingTraceIDs must be empty at this point; got %d items: %d", len(tmm.pendingTraceIDs), tmm.pendingTraceIDs)
	}
	if !checkItemsSorted(dstItems) {
		// Items could become unsorted if initial items contain duplicate metricIDs:
		//
		//   item1: 1, 1, 5
		//   item2: 1, 4
		//
		// Items could become the following after the merge:
		//
		//   item1: 1, 5
		//   item2: 1, 4
		//
		// i.e. item1 > item2
		//
		// Leave the original items unmerged, so they can be merged next time.
		// This case should be quite rare - if multiple data points are simultaneously inserted
		// into the same new time series from multiple concurrent goroutines.
		atomic.AddUint64(&indexBlocksWithTraceIDsIncorrectOrder, 1)
		dstData = append(dstData[:0], tmm.dataCopy...)
		dstItems = dstItems[:0]
		// tmm.itemsCopy can point to overwritten data, so it must be updated
		// to point to real data from tmm.dataCopy.
		buf := dstData
		for _, item := range tmm.itemsCopy {
			dstItems = append(dstItems, buf[:len(item)])
			buf = buf[len(item):]
		}
		if !checkItemsSorted(dstItems) {
			logger.Panicf("BUG: the original items weren't sorted; items=%q", dstItems)
		}
	}
	putTagToTraceIDsRowsMerger(tmm)
	atomic.AddUint64(&indexBlocksWithTraceIDsProcessed, 1)
	return dstData, dstItems
}

var indexBlocksWithTraceIDsIncorrectOrder uint64
var indexBlocksWithTraceIDsProcessed uint64

func checkItemsSorted(items [][]byte) bool {
	if len(items) == 0 {
		return true
	}
	prevItem := items[0]
	for _, currItem := range items[1:] {
		if string(prevItem) > string(currItem) {
			return false
		}
		prevItem = currItem
	}
	return true
}

// maxTraceIDsPerRow limits the number of metricIDs in tag->metricIDs row.
//
// This reduces overhead on index and metaindex in lib/mergeset.
const maxTraceIDsPerRow = 64

type traceIDSorter []TraceID

func (s traceIDSorter) Len() int { return len(s) }
func (s traceIDSorter) Less(i, j int) bool {
	return s[i].Less(s[j])
}
func (s traceIDSorter) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type tagToTraceIDsRowsMerger struct {
	pendingTraceIDs traceIDSorter
	mp              tagToTraceIDsRowParser
	mpPrev          tagToTraceIDsRowParser

	itemsCopy [][]byte
	dataCopy  []byte
}

func (tmm *tagToTraceIDsRowsMerger) Reset() {
	tmm.pendingTraceIDs = tmm.pendingTraceIDs[:0]
	tmm.mp.Reset()
	tmm.mpPrev.Reset()

	tmm.itemsCopy = tmm.itemsCopy[:0]
	tmm.dataCopy = tmm.dataCopy[:0]
}

func (tmm *tagToTraceIDsRowsMerger) flushPendingTraceIDs(dstData []byte, dstItems [][]byte, mp *tagToTraceIDsRowParser) ([]byte, [][]byte) {
	if len(tmm.pendingTraceIDs) == 0 {
		// Nothing to flush
		return dstData, dstItems
	}
	// Use sort.Sort instead of sort.Slice in order to reduce memory allocations.
	sort.Sort(&tmm.pendingTraceIDs)
	tmm.pendingTraceIDs = removeDuplicateTraceIDs(tmm.pendingTraceIDs)

	// Marshal pendingTraceIDs
	dstDataLen := len(dstData)
	dstData = mp.MarshalPrefix(dstData)
	for _, traceID := range tmm.pendingTraceIDs {
		dstData = traceID.Marshal(dstData)
	}
	dstItems = append(dstItems, dstData[dstDataLen:])
	tmm.pendingTraceIDs = tmm.pendingTraceIDs[:0]
	return dstData, dstItems
}

func removeDuplicateTraceIDs(sortedTraceIDs []TraceID) []TraceID {
	if len(sortedTraceIDs) < 2 {
		return sortedTraceIDs
	}
	prevTraceID := sortedTraceIDs[0]
	hasDuplicates := false
	for _, traceID := range sortedTraceIDs[1:] {
		if prevTraceID == traceID {
			hasDuplicates = true
			break
		}
		prevTraceID = traceID
	}
	if !hasDuplicates {
		return sortedTraceIDs
	}
	dstTraceIDs := sortedTraceIDs[:1]
	prevTraceID = sortedTraceIDs[0]
	for _, traceID := range sortedTraceIDs[1:] {
		if prevTraceID == traceID {
			continue
		}
		dstTraceIDs = append(dstTraceIDs, traceID)
		prevTraceID = traceID
	}
	return dstTraceIDs
}

func getTagToTraceIDsRowsMerger() *tagToTraceIDsRowsMerger {
	v := tmmPool.Get()
	if v == nil {
		return &tagToTraceIDsRowsMerger{}
	}
	return v.(*tagToTraceIDsRowsMerger)
}

func putTagToTraceIDsRowsMerger(tmm *tagToTraceIDsRowsMerger) {
	tmm.Reset()
	tmmPool.Put(tmm)
}

var tmmPool sync.Pool
