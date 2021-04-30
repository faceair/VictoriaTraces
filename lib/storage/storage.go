package storage

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storagepacelimiter"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/timerpool"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/uint64set"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/workingsetcache"
	"github.com/VictoriaMetrics/fastcache"
	"github.com/faceair/VictoriaTraces/lib/uint128"
)

const (
	msecsPerMonth     = 31 * 24 * 3600 * 1000
	maxRetentionMsecs = 100 * 12 * msecsPerMonth
)

// Storage represents TSDB storage.
type Storage struct {
	// Atomic counters must go at the top of the structure in order to properly align by 8 bytes on 32-bit archs.
	// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/212 .
	tooSmallTimestampRows uint64
	tooBigTimestampRows   uint64

	addRowsConcurrencyLimitReached uint64
	addRowsConcurrencyLimitTimeout uint64
	addRowsConcurrencyDroppedRows  uint64

	searchTSIDsConcurrencyLimitReached uint64
	searchTSIDsConcurrencyLimitTimeout uint64

	slowRowInserts         uint64
	slowPerDayIndexInserts uint64
	slowMetricNameLoads    uint64

	path            string
	cachePath       string
	retentionMonths int

	// lock file for exclusive access to the storage on the given path.
	flockF *os.File

	idbCurr atomic.Value

	tb *table

	// metricNameCache is MetricID -> MetricName cache.
	metricIDCache   *workingsetcache.Cache
	metricNameCache *workingsetcache.Cache

	// Fast cache for TraceID values occurred during the current minute.
	currHourTraceIDs atomic.Value

	// Fast cache for TraceID values occurred during the previous minute.
	prevHourTraceIDs atomic.Value

	// Pending TraceID values to be added to currHourTraceIDs.
	pendingHourEntriesLock sync.Mutex
	pendingHourEntries     []TraceID

	// Pending TraceID to be added to nextHourTraceIDs.
	pendingNextHourTraceIDsLock sync.Mutex
	pendingNextHourTraceIDs     *uint64set.Set
	stop                        chan struct{}

	currHourTraceIDsUpdaterWG sync.WaitGroup
	nextHourTraceIDsUpdaterWG sync.WaitGroup
	retentionWatcherWG        sync.WaitGroup
}

// OpenStorage opens storage on the given path with the given retentionMsecs.
func OpenStorage(path string, retentionMsecs int64) (*Storage, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("cannot determine absolute path for %q: %w", path, err)
	}
	if retentionMsecs <= 0 {
		retentionMsecs = maxRetentionMsecs
	}
	retentionMonths := (retentionMsecs + (msecsPerMonth - 1)) / msecsPerMonth
	s := &Storage{
		path:            path,
		cachePath:       path + "/cache",
		retentionMonths: int(retentionMonths),
		stop:            make(chan struct{}),
	}

	if err := fs.MkdirAllIfNotExist(path); err != nil {
		return nil, fmt.Errorf("cannot create a directory for the storage at %q: %w", path, err)
	}

	// Protect from concurrent opens.
	flockF, err := fs.CreateFlockFile(path)
	if err != nil {
		return nil, err
	}
	s.flockF = flockF

	// Load caches.
	mem := memory.Allowed()
	s.metricIDCache = s.mustLoadCache("MetricName->MetricID", "metricName_metricID", mem/8)
	s.metricNameCache = s.mustLoadCache("MetricID->MetricName", "metricID_metricName", mem/8)

	hour := fasttime.UnixHour()
	hmCurr := s.mustLoadHourTraceIDs(hour, "curr_hour_trace_ids")
	hmPrev := s.mustLoadHourTraceIDs(hour-1, "prev_hour_trace_ids")
	s.currHourTraceIDs.Store(hmCurr)
	s.prevHourTraceIDs.Store(hmPrev)
	s.pendingNextHourTraceIDs = &uint64set.Set{}

	// Load indexdb
	idbPath := path + "/indexdb"
	idbCurr, idbPrev, err := openIndexDBTables(idbPath, s.metricIDCache, s.metricNameCache)
	if err != nil {
		return nil, fmt.Errorf("cannot open indexdb tables at %q: %w", idbPath, err)
	}
	idbCurr.SetExtDB(idbPrev)
	s.idbCurr.Store(idbCurr)

	// Load data
	tablePath := path + "/data"
	tb, err := openTable(tablePath, retentionMsecs)
	if err != nil {
		s.idb().MustClose()
		return nil, fmt.Errorf("cannot open table at %q: %w", tablePath, err)
	}
	s.tb = tb

	s.startCurrHourMetricIDsUpdater()
	s.startRetentionWatcher()

	return s, nil
}

// RetentionMonths returns retention months for s.
func (s *Storage) RetentionMonths() int {
	return s.retentionMonths
}

// debugFlush flushes recently added storage data, so it becomes visible to search.
func (s *Storage) debugFlush() {
	s.tb.flushRawRows()
	s.idb().tb.DebugFlush()
}

func (s *Storage) idb() *indexDB {
	return s.idbCurr.Load().(*indexDB)
}

// Metrics contains essential metrics for the Storage.
type Metrics struct {
	RowsAddedTotal uint64

	TooSmallTimestampRows uint64
	TooBigTimestampRows   uint64

	AddRowsConcurrencyLimitReached uint64
	AddRowsConcurrencyLimitTimeout uint64
	AddRowsConcurrencyDroppedRows  uint64
	AddRowsConcurrencyCapacity     uint64
	AddRowsConcurrencyCurrent      uint64

	SearchTSIDsConcurrencyLimitReached uint64
	SearchTSIDsConcurrencyLimitTimeout uint64
	SearchTSIDsConcurrencyCapacity     uint64
	SearchTSIDsConcurrencyCurrent      uint64

	SearchDelays uint64

	SlowRowInserts         uint64
	SlowPerDayIndexInserts uint64
	SlowMetricNameLoads    uint64

	TimestampsBlocksMerged uint64
	TimestampsBytesSaved   uint64

	MetricNameCacheSize       uint64
	MetricNameCacheSizeBytes  uint64
	MetricNameCacheRequests   uint64
	MetricNameCacheMisses     uint64
	MetricNameCacheCollisions uint64

	HourMetricIDCacheSize      uint64
	HourMetricIDCacheSizeBytes uint64

	IndexDBMetrics IndexDBMetrics
	TableMetrics   TableMetrics
}

// Reset resets m.
func (m *Metrics) Reset() {
	*m = Metrics{}
}

// UpdateMetrics updates m with metrics from s.
func (s *Storage) UpdateMetrics(m *Metrics) {
	m.RowsAddedTotal = atomic.LoadUint64(&rowsAddedTotal)

	m.TooSmallTimestampRows += atomic.LoadUint64(&s.tooSmallTimestampRows)
	m.TooBigTimestampRows += atomic.LoadUint64(&s.tooBigTimestampRows)

	m.AddRowsConcurrencyLimitReached += atomic.LoadUint64(&s.addRowsConcurrencyLimitReached)
	m.AddRowsConcurrencyLimitTimeout += atomic.LoadUint64(&s.addRowsConcurrencyLimitTimeout)
	m.AddRowsConcurrencyDroppedRows += atomic.LoadUint64(&s.addRowsConcurrencyDroppedRows)
	m.AddRowsConcurrencyCapacity = uint64(cap(addRowsConcurrencyCh))
	m.AddRowsConcurrencyCurrent = uint64(len(addRowsConcurrencyCh))

	m.SearchTSIDsConcurrencyLimitReached += atomic.LoadUint64(&s.searchTSIDsConcurrencyLimitReached)
	m.SearchTSIDsConcurrencyLimitTimeout += atomic.LoadUint64(&s.searchTSIDsConcurrencyLimitTimeout)
	m.SearchTSIDsConcurrencyCapacity = uint64(cap(searchTSIDsConcurrencyCh))
	m.SearchTSIDsConcurrencyCurrent = uint64(len(searchTSIDsConcurrencyCh))

	m.SearchDelays = storagepacelimiter.Search.DelaysTotal()

	m.SlowRowInserts += atomic.LoadUint64(&s.slowRowInserts)
	m.SlowPerDayIndexInserts += atomic.LoadUint64(&s.slowPerDayIndexInserts)
	m.SlowMetricNameLoads += atomic.LoadUint64(&s.slowMetricNameLoads)

	m.TimestampsBlocksMerged = atomic.LoadUint64(&timestampsBlocksMerged)
	m.TimestampsBytesSaved = atomic.LoadUint64(&timestampsBytesSaved)

	var cs fastcache.Stats

	cs.Reset()
	s.metricNameCache.UpdateStats(&cs)
	m.MetricNameCacheSize += cs.EntriesCount
	m.MetricNameCacheSizeBytes += cs.BytesSize
	m.MetricNameCacheRequests += cs.GetCalls
	m.MetricNameCacheMisses += cs.Misses
	m.MetricNameCacheCollisions += cs.Collisions

	hmCurr := s.currHourTraceIDs.Load().(*hourTraceIDs)
	hmPrev := s.prevHourTraceIDs.Load().(*hourTraceIDs)
	hourMetricIDsLen := hmPrev.m.Len()
	if hmCurr.m.Len() > hourMetricIDsLen {
		hourMetricIDsLen = hmCurr.m.Len()
	}
	m.HourMetricIDCacheSize += uint64(hourMetricIDsLen)
	m.HourMetricIDCacheSizeBytes += hmCurr.m.SizeBytes()
	m.HourMetricIDCacheSizeBytes += hmPrev.m.SizeBytes()

	s.idb().UpdateMetrics(&m.IndexDBMetrics)
	s.tb.UpdateMetrics(&m.TableMetrics)
}

func (s *Storage) startRetentionWatcher() {
	s.retentionWatcherWG.Add(1)
	go func() {
		s.retentionWatcher()
		s.retentionWatcherWG.Done()
	}()
}

func (s *Storage) retentionWatcher() {
	for {
		d := nextRetentionDuration(s.retentionMonths)
		select {
		case <-s.stop:
			return
		case <-time.After(d):
			s.mustRotateIndexDB()
		}
	}
}

func (s *Storage) startCurrHourMetricIDsUpdater() {
	s.currHourTraceIDsUpdaterWG.Add(1)
	go func() {
		s.currHourMetricIDsUpdater()
		s.currHourTraceIDsUpdaterWG.Done()
	}()
}

var currHourMetricIDsUpdateInterval = time.Second * 10

func (s *Storage) currHourMetricIDsUpdater() {
	ticker := time.NewTicker(currHourMetricIDsUpdateInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			s.updateCurrHourTraceIDs()
			return
		case <-ticker.C:
			s.updateCurrHourTraceIDs()
		}
	}
}

func (s *Storage) mustRotateIndexDB() {
	// Create new indexdb table.
	newTableName := nextIndexDBTableName()
	idbNewPath := s.path + "/indexdb/" + newTableName
	idbNew, err := openIndexDB(idbNewPath, s.metricIDCache, s.metricNameCache)
	if err != nil {
		logger.Panicf("FATAL: cannot create new indexDB at %q: %s", idbNewPath, err)
	}

	// Drop extDB
	idbCurr := s.idb()
	idbCurr.doExtDB(func(extDB *indexDB) {
		extDB.scheduleToDrop()
	})
	idbCurr.SetExtDB(nil)

	// Start using idbNew
	idbNew.SetExtDB(idbCurr)
	s.idbCurr.Store(idbNew)

	// Persist changes on the file system.
	fs.MustSyncPath(s.path)

	// Do not flush metricNameCache, since all the metricIDs
	// from prev idb remain valid after the rotation.
}

// MustClose closes the storage.
func (s *Storage) MustClose() {
	close(s.stop)

	s.retentionWatcherWG.Wait()
	s.currHourTraceIDsUpdaterWG.Wait()
	s.nextHourTraceIDsUpdaterWG.Wait()

	s.tb.MustClose()
	s.idb().MustClose()

	// Save caches.
	s.mustSaveAndStopCache(s.metricIDCache, "MetricName->MetricID", "metricName_metricID")
	s.mustSaveAndStopCache(s.metricNameCache, "MetricID->MetricName", "metricID_metricName")

	hmCurr := s.currHourTraceIDs.Load().(*hourTraceIDs)
	s.mustSaveHourTraceIDs(hmCurr, "curr_hour_trace_ids")
	hmPrev := s.prevHourTraceIDs.Load().(*hourTraceIDs)
	s.mustSaveHourTraceIDs(hmPrev, "prev_hour_trace_ids")

	// Release lock file.
	if err := s.flockF.Close(); err != nil {
		logger.Panicf("FATAL: cannot close lock file %q: %s", s.flockF.Name(), err)
	}
}

func (s *Storage) mustLoadHourTraceIDs(hour uint64, name string) *hourTraceIDs {
	hm := &hourTraceIDs{
		hour: hour,
	}
	path := s.cachePath + "/" + name
	logger.Infof("loading %s from %q...", name, path)
	startTime := time.Now()
	if !fs.IsPathExist(path) {
		logger.Infof("nothing to load from %q", path)
		return hm
	}
	src, err := ioutil.ReadFile(path)
	if err != nil {
		logger.Panicf("FATAL: cannot read %s: %s", path, err)
	}
	srcOrigLen := len(src)
	if len(src) < 24 {
		logger.Errorf("discarding %s, since it has broken header; got %d bytes; want %d bytes", path, len(src), 24)
		return hm
	}

	// Unmarshal header
	isFull := encoding.UnmarshalUint64(src)
	src = src[8:]
	hourLoaded := encoding.UnmarshalUint64(src)
	src = src[8:]
	if hourLoaded != hour {
		logger.Infof("discarding %s, since it contains outdated hour; got %d; want %d", path, hourLoaded, hour)
		return hm
	}

	// Unmarshal uint64set
	m, tail, err := unmarshalUint128Set(src)
	if err != nil {
		logger.Infof("discarding %s because cannot load uint64set: %s", path, err)
		return hm
	}
	src = tail

	hm.m = m
	hm.isFull = isFull != 0
	logger.Infof("loaded %s from %q in %.3f seconds; entriesCount: %d; sizeBytes: %d", name, path, time.Since(startTime).Seconds(), m.Len(), srcOrigLen)
	return hm
}

func (s *Storage) mustSaveHourTraceIDs(hm *hourTraceIDs, name string) {
	path := s.cachePath + "/" + name
	logger.Infof("saving %s to %q...", name, path)
	startTime := time.Now()
	dst := make([]byte, 0, hm.m.Len()*8+24)
	isFull := uint64(0)
	if hm.isFull {
		isFull = 1
	}

	// Marshal header
	dst = encoding.MarshalUint64(dst, isFull)
	dst = encoding.MarshalUint64(dst, hm.hour)

	// Marshal hm.m
	dst = marshalUint128Set(dst, hm.m)

	if err := ioutil.WriteFile(path, dst, 0644); err != nil {
		logger.Panicf("FATAL: cannot write %d bytes to %q: %s", len(dst), path, err)
	}
	logger.Infof("saved %s to %q in %.3f seconds; entriesCount: %d; sizeBytes: %d", name, path, time.Since(startTime).Seconds(), hm.m.Len(), len(dst))
}

func unmarshalUint128Set(src []byte) (*uint128.Set, []byte, error) {
	mLen := encoding.UnmarshalUint64(src)
	src = src[8:]
	if uint64(len(src)) < 8*mLen {
		return nil, nil, fmt.Errorf("cannot unmarshal uint64set; got %d bytes; want at least %d bytes", len(src), 8*mLen)
	}
	var err error
	var traceID TraceID
	m := &uint128.Set{}
	for i := uint64(0); i < mLen; i++ {
		traceID, src, err = uint128.Unmarshal(src)
		if err != nil {
			return nil, nil, fmt.Errorf("cannot unmarshal traceid: %s", err)
		}
		m.Add(traceID)
	}
	return m, src, nil
}

func marshalUint128Set(dst []byte, m *uint128.Set) []byte {
	dst = encoding.MarshalUint64(dst, uint64(m.Len()))
	m.ForEach(func(part []uint128.Uint128) bool {
		for _, traceID := range part {
			dst = traceID.Marshal(dst)
		}
		return true
	})
	return dst
}

func (s *Storage) mustLoadCache(info, name string, sizeBytes int) *workingsetcache.Cache {
	path := s.cachePath + "/" + name
	logger.Infof("loading %s cache from %q...", info, path)
	startTime := time.Now()
	c := workingsetcache.Load(path, sizeBytes, time.Hour)
	var cs fastcache.Stats
	c.UpdateStats(&cs)
	logger.Infof("loaded %s cache from %q in %.3f seconds; entriesCount: %d; sizeBytes: %d",
		info, path, time.Since(startTime).Seconds(), cs.EntriesCount, cs.BytesSize)
	return c
}

func (s *Storage) mustSaveAndStopCache(c *workingsetcache.Cache, info, name string) {
	path := s.cachePath + "/" + name
	logger.Infof("saving %s cache to %q...", info, path)
	startTime := time.Now()
	if err := c.Save(path); err != nil {
		logger.Panicf("FATAL: cannot save %s cache to %q: %s", info, path, err)
	}
	var cs fastcache.Stats
	c.UpdateStats(&cs)
	c.Stop()
	logger.Infof("saved %s cache to %q in %.3f seconds; entriesCount: %d; sizeBytes: %d",
		info, path, time.Since(startTime).Seconds(), cs.EntriesCount, cs.BytesSize)
}

func nextRetentionDuration(retentionMonths int) time.Duration {
	t := time.Now().UTC()
	n := t.Year()*12 + int(t.Month()) - 1 + retentionMonths
	n -= n % retentionMonths
	y := n / 12
	m := time.Month((n % 12) + 1)
	// Schedule the deadline to +4 hours from the next retention period start.
	// This should prevent from possible double deletion of indexdb
	// due to time drift - see https://github.com/VictoriaMetrics/VictoriaMetrics/issues/248 .
	deadline := time.Date(y, m, 1, 4, 0, 0, 0, time.UTC)
	return deadline.Sub(t)
}

// searchTraceIDs returns sorted TSIDs for the given tfss and the given tr.
func (s *Storage) searchTraceIDs(tfss []*TagFilters, tr ScanRange, deadline uint64) ([]TraceID, error) {
	// Do not cache tfss -> tsids here, since the caching is performed
	// on idb level.

	// Limit the number of concurrent goroutines that may search TSIDS in the storage.
	// This should prevent from out of memory errors and CPU trashing when too many
	// goroutines call searchTraceIDs.
	select {
	case searchTSIDsConcurrencyCh <- struct{}{}:
	default:
		// Sleep for a while until giving up
		atomic.AddUint64(&s.searchTSIDsConcurrencyLimitReached, 1)
		currentTime := fasttime.UnixTimestamp()
		timeoutSecs := uint64(0)
		if currentTime < deadline {
			timeoutSecs = deadline - currentTime
		}
		timeout := time.Second * time.Duration(timeoutSecs)
		t := timerpool.Get(timeout)
		select {
		case searchTSIDsConcurrencyCh <- struct{}{}:
			timerpool.Put(t)
		case <-t.C:
			timerpool.Put(t)
			atomic.AddUint64(&s.searchTSIDsConcurrencyLimitTimeout, 1)
			return nil, fmt.Errorf("cannot search for tsids, since more than %d concurrent searches are performed during %.3f secs; add more CPUs or reduce query load",
				cap(searchTSIDsConcurrencyCh), timeout.Seconds())
		}
	}
	tsids, err := s.idb().searchTraceIDs(tfss, tr, deadline)
	<-searchTSIDsConcurrencyCh
	if err != nil {
		return nil, fmt.Errorf("error when searching tsids: %w", err)
	}
	return tsids, nil
}

var (
	// Limit the concurrency for TraceID searches to GOMAXPROCS*2, since this operation
	// is CPU bound and sometimes disk IO bound, so there is no sense in running more
	// than GOMAXPROCS*2 concurrent goroutines for TraceID searches.
	searchTSIDsConcurrencyCh = make(chan struct{}, runtime.GOMAXPROCS(-1)*2)
)

// ErrDeadlineExceeded is returned when the request times out.
var ErrDeadlineExceeded = fmt.Errorf("deadline exceeded")

// SearchTagKeys searches for tag keys for the given (accountID, projectID).
func (s *Storage) SearchTagKeys(maxTagKeys int, deadline uint64) ([]string, error) {
	return s.idb().SearchTagKeys(maxTagKeys, deadline)
}

// SearchTagValues searches for tag values for the given tagKey in (accountID, projectID).
func (s *Storage) SearchTagValues(tagKey []byte, maxTagValues int, deadline uint64) ([]string, error) {
	return s.idb().SearchTagValues(tagKey, maxTagValues, deadline)
}

// SearchTagEntries returns a list of (tagName -> tagValues) for (accountID, projectID).
func (s *Storage) SearchTagEntries(maxTagKeys, maxTagValues int, deadline uint64) ([]TagEntry, error) {
	idb := s.idb()
	keys, err := idb.SearchTagKeys(maxTagKeys, deadline)
	if err != nil {
		return nil, fmt.Errorf("cannot search tag keys: %w", err)
	}

	// Sort keys for faster seeks below
	sort.Strings(keys)

	tes := make([]TagEntry, len(keys))
	for i, key := range keys {
		values, err := idb.SearchTagValues([]byte(key), maxTagValues, deadline)
		if err != nil {
			return nil, fmt.Errorf("cannot search values for tag %q: %w", key, err)
		}
		te := &tes[i]
		te.Key = key
		te.Values = values
	}
	return tes, nil
}

// TagEntry contains (tagName -> tagValues) mapping
type TagEntry struct {
	// Key is tagName
	Key string

	// Values contains all the values for Key.
	Values []string
}

// SpanRow is a metric to insert into storage.
type SpanRow struct {
	// SpanNameRaw contains raw metric name, which must be decoded
	// with SpanName.unmarshalRaw.
	SpanNameRaw []byte

	Timestamp int64
	Value     []byte
}

// CopyFrom copies src to mr.
func (mr *SpanRow) CopyFrom(src *SpanRow) {
	mr.SpanNameRaw = append(mr.SpanNameRaw[:0], src.SpanNameRaw...)
	mr.Timestamp = src.Timestamp
	mr.Value = src.Value
}

// String returns string representation of the mr.
func (mr *SpanRow) String() string {
	metricName := string(mr.SpanNameRaw)
	var mn SpanName
	if err := mn.unmarshalRaw(mr.SpanNameRaw); err == nil {
		metricName = mn.String()
	}
	return fmt.Sprintf("SpanName=%s, Timestamp=%d, Value=%v\n", metricName, mr.Timestamp, mr.Value)
}

// Marshal appends marshaled mr to dst and returns the result.
func (mr *SpanRow) Marshal(dst []byte) []byte {
	return MarshalMetricRow(dst, mr.SpanNameRaw, mr.Timestamp, mr.Value)
}

// MarshalMetricRow marshals SpanRow data to dst and returns the result.
func MarshalMetricRow(dst []byte, metricNameRaw []byte, timestamp int64, value []byte) []byte {
	dst = encoding.MarshalBytes(dst, metricNameRaw)
	dst = encoding.MarshalUint64(dst, uint64(timestamp))
	dst = encoding.MarshalBytes(dst, value)
	return dst
}

// Unmarshal unmarshals mr from src and returns the remaining tail from src.
func (mr *SpanRow) Unmarshal(src []byte) ([]byte, error) {
	tail, metricNameRaw, err := encoding.UnmarshalBytes(src)
	if err != nil {
		return tail, fmt.Errorf("cannot unmarshal SpanName: %w", err)
	}
	mr.SpanNameRaw = append(mr.SpanNameRaw[:0], metricNameRaw...)

	if len(tail) < 8 {
		return tail, fmt.Errorf("cannot unmarshal Timestamp: want %d bytes; have %d bytes", 8, len(tail))
	}
	timestamp := encoding.UnmarshalUint64(tail)
	mr.Timestamp = int64(timestamp)
	tail = tail[8:]

	tail, value, err := encoding.UnmarshalBytes(tail)
	if err != nil {
		return tail, fmt.Errorf("cannot unmarshal value: %w", err)
	}
	mr.Value = value

	return tail, nil
}

// ForceMergePartitions force-merges partitions in s with names starting from the given partitionNamePrefix.
//
// Partitions are merged sequentially in order to reduce load on the system.
func (s *Storage) ForceMergePartitions(partitionNamePrefix string) error {
	return s.tb.ForceMergePartitions(partitionNamePrefix)
}

var rowsAddedTotal uint64

// AddRows adds the given mrs to s.
func (s *Storage) AddRows(mrs []SpanRow) error {
	if len(mrs) == 0 {
		return nil
	}
	atomic.AddUint64(&rowsAddedTotal, uint64(len(mrs)))

	// Limit the number of concurrent goroutines that may add rows to the storage.
	// This should prevent from out of memory errors and CPU trashing when too many
	// goroutines call AddRows.
	select {
	case addRowsConcurrencyCh <- struct{}{}:
	default:
		// Sleep for a while until giving up
		atomic.AddUint64(&s.addRowsConcurrencyLimitReached, 1)
		t := timerpool.Get(addRowsTimeout)

		// Prioritize data ingestion over concurrent searches.
		storagepacelimiter.Search.Inc()

		select {
		case addRowsConcurrencyCh <- struct{}{}:
			timerpool.Put(t)
			storagepacelimiter.Search.Dec()
		case <-t.C:
			timerpool.Put(t)
			storagepacelimiter.Search.Dec()
			atomic.AddUint64(&s.addRowsConcurrencyLimitTimeout, 1)
			atomic.AddUint64(&s.addRowsConcurrencyDroppedRows, uint64(len(mrs)))
			return fmt.Errorf("cannot add %d rows to storage in %s, since it is overloaded with %d concurrent writers; add more CPUs or reduce load",
				len(mrs), addRowsTimeout, cap(addRowsConcurrencyCh))
		}
	}

	// Add rows to the storage.
	var err error
	rr := getRawRowsWithSize(len(mrs))
	rr.rows, err = s.add(rr.rows, mrs)
	putRawRows(rr)

	<-addRowsConcurrencyCh

	return err
}

var (
	// Limit the concurrency for data ingestion to GOMAXPROCS, since this operation
	// is CPU bound, so there is no sense in running more than GOMAXPROCS concurrent
	// goroutines on data ingestion path.
	addRowsConcurrencyCh = make(chan struct{}, runtime.GOMAXPROCS(-1))
	addRowsTimeout       = 30 * time.Second
)

func (s *Storage) add(rows []rawRow, mrs []SpanRow) ([]rawRow, error) {
	idb := s.idb()
	rowsLen := len(rows)
	if n := rowsLen + len(mrs) - cap(rows); n > 0 {
		rows = append(rows[:cap(rows)], make([]rawRow, n)...)
	}
	rows = rows[:rowsLen+len(mrs)]
	j := 0
	var (
		// These vars are used for speeding up bulk imports of multiple adjancent rows for the same metricName.
		prevTraceID     TraceID
		prevMetricID    uint64
		prevSpanNameRaw []byte
	)
	var pmrs *pendingSpanRows
	minTimestamp, maxTimestamp := s.tb.getMinMaxTimestamps()
	// Return only the first error, since it has no sense in returning all errors.
	var firstWarn error
	for i := range mrs {
		mr := &mrs[i]
		if len(mr.Value) == 0 {
			// Just skip NaNs, since the underlying encoding
			// doesn't know how to work with them.
			continue
		}
		if mr.Timestamp < minTimestamp {
			// Skip rows with too small timestamps outside the retention.
			if firstWarn == nil {
				firstWarn = fmt.Errorf("cannot insert row with too small timestamp %d outside the retention; minimum allowed timestamp is %d; "+
					"probably you need updating -retentionPeriod command-line flag",
					mr.Timestamp, minTimestamp)
			}
			atomic.AddUint64(&s.tooSmallTimestampRows, 1)
			continue
		}
		if mr.Timestamp > maxTimestamp {
			// Skip rows with too big timestamps significantly exceeding the current time.
			if firstWarn == nil {
				firstWarn = fmt.Errorf("cannot insert row with too big timestamp %d exceeding the current time; maximum allowd timestamp is %d; "+
					"propbably you need updating -retentionPeriod command-line flag",
					mr.Timestamp, maxTimestamp)
			}
			atomic.AddUint64(&s.tooBigTimestampRows, 1)
			continue
		}
		r := &rows[rowsLen+j]
		j++
		r.Timestamp = mr.Timestamp
		r.Value = mr.Value

		if string(mr.SpanNameRaw) == string(prevSpanNameRaw) {
			// Fast path - the current mr contains the same metric name as the previous mr, so it contains the same TraceID.
			// This path should trigger on bulk imports when many rows contain the same SpanNameRaw.
			r.TraceID = prevTraceID
			r.MetricID = prevMetricID
			continue
		}
		// Slow path - the TraceID is missing in the cache.
		// Postpone its search in the loop below.
		j--
		if pmrs == nil {
			pmrs = getPendingMetricRows()
		}
		if err := pmrs.addRow(mr); err != nil {
			// Do not stop adding rows on error - just skip invalid row.
			// This guarantees that invalid rows don't prevent
			// from adding valid rows into the storage.
			if firstWarn == nil {
				firstWarn = err
			}
			continue
		}
	}
	if pmrs != nil {
		// Sort pendingSpanRows by canonical metric name in order to speed up search via `is` in the loop below.
		is := idb.getIndexSearch(noDeadline)
		mn := GetSpanName()
		prevSpanNameRaw = nil
		var slowInsertsCount uint64
		var metricID uint64
		for i := range pmrs.pmrs {
			pmr := &pmrs.pmrs[i]
			mr := &pmr.mr
			r := &rows[rowsLen+j]
			j++
			r.Timestamp = mr.Timestamp
			r.Value = mr.Value
			if string(mr.SpanNameRaw) == string(prevSpanNameRaw) {
				// Fast path - the current mr contains the same metric name as the previous mr, so it contains the same TraceID.
				// This path should trigger on bulk imports when many rows contain the same SpanNameRaw.
				r.TraceID = prevTraceID
				continue
			}
			metricName, err := mn.unmarshalTraceID(mr.SpanNameRaw)
			if err != nil {
				if firstWarn == nil {
					firstWarn = err
				}
				continue
			}
			r.TraceID, err = parseTraceID(bytesutil.ToUnsafeString(mn.TraceID))
			if err != nil {
				if firstWarn == nil {
					firstWarn = err
				}
				continue
			}
			metricID, err = is.db.getMetricIDFromCache(metricName)
			if err == nil {
				r.MetricID = metricID
				continue
			}
			if err != io.EOF {
				if firstWarn == nil {
					firstWarn = err
				}
				continue
			}
			slowInsertsCount++
			metricID, err = is.GetOrCreateMetricID(metricName)
			if err != nil {
				if firstWarn == nil {
					firstWarn = err
				}
				continue
			}
			r.MetricID = metricID
		}
		PutSpanName(mn)
		idb.putIndexSearch(is)
		putPendingMetricRows(pmrs)
		atomic.AddUint64(&s.slowRowInserts, slowInsertsCount)
	}
	if firstWarn != nil {
		logger.Errorf("warn occurred during rows addition: %s", firstWarn)
	}
	rows = rows[:rowsLen+j]

	var firstError error
	if err := s.tb.AddRows(rows); err != nil {
		firstError = fmt.Errorf("cannot add rows to table: %w", err)
	}
	if err := s.addIndex(rows); err != nil && firstError == nil {
		firstError = fmt.Errorf("cannot update per-date data: %w", err)
	}
	if firstError != nil {
		return rows, fmt.Errorf("error occurred during rows addition: %w", firstError)
	}
	return rows, nil
}

type pendingSpanRow struct {
	SpanName []byte
	mr       SpanRow
}

type pendingSpanRows struct {
	pmrs         []pendingSpanRow
	spanNamesBuf []byte

	lastSpanNameRaw []byte
	lastSpanName    []byte
	mn              SpanName
}

func (pmrs *pendingSpanRows) reset() {
	for _, pmr := range pmrs.pmrs {
		pmr.SpanName = nil
		pmr.mr.SpanNameRaw = nil
	}
	pmrs.pmrs = pmrs.pmrs[:0]
	pmrs.spanNamesBuf = pmrs.spanNamesBuf[:0]
	pmrs.lastSpanNameRaw = nil
	pmrs.lastSpanName = nil
	pmrs.mn.Reset()
}

func (pmrs *pendingSpanRows) addRow(mr *SpanRow) error {
	// Do not spend CPU time on re-calculating canonical metricName during bulk import
	// of many rows for the same metric.
	if string(mr.SpanNameRaw) != string(pmrs.lastSpanNameRaw) {
		if err := pmrs.mn.unmarshalRaw(mr.SpanNameRaw); err != nil {
			return fmt.Errorf("cannot unmarshal SpanNameRaw %q: %w", mr.SpanNameRaw, err)
		}
		pmrs.mn.sortTags()
		metricNamesBufLen := len(pmrs.spanNamesBuf)
		pmrs.spanNamesBuf = pmrs.mn.Marshal(pmrs.spanNamesBuf)
		pmrs.lastSpanName = pmrs.spanNamesBuf[metricNamesBufLen:]
		pmrs.lastSpanNameRaw = mr.SpanNameRaw
	}
	pmrs.pmrs = append(pmrs.pmrs, pendingSpanRow{
		SpanName: pmrs.lastSpanName,
		mr:       *mr,
	})
	return nil
}

func getPendingMetricRows() *pendingSpanRows {
	v := pendingMetricRowsPool.Get()
	if v == nil {
		v = &pendingSpanRows{}
	}
	return v.(*pendingSpanRows)
}

func putPendingMetricRows(pmrs *pendingSpanRows) {
	pmrs.reset()
	pendingMetricRowsPool.Put(pmrs)
}

var pendingMetricRowsPool sync.Pool

func (s *Storage) addIndex(rows []rawRow) error {
	var hour uint64
	var (
		// These vars are used for speeding up bulk imports when multiple adjancent rows
		// contain the same (metricID, date) pairs.
		prevHour, prevTimestamp uint64
		prevTraceID             TraceID
	)
	hm := s.currHourTraceIDs.Load().(*hourTraceIDs)
	type pendingSpan struct {
		timestamp uint64
		metricID  uint64
		traceID   TraceID
	}
	var pendingHourSpans []pendingSpan
	for i := range rows {
		r := &rows[i]
		traceID := r.TraceID
		metricID := r.MetricID
		timestamp := uint64(r.Timestamp)
		if timestamp != prevTimestamp {
			hour = timestamp / msecPerHour
			prevTimestamp = timestamp
		}
		if hour == hm.hour {
			// The r belongs to the current hour. Check for the current hour cache.
			if hm.m.Has(traceID) {
				// Fast path: the traceID is in the current hour cache.
				// This means the traceID has been already added to per-day inverted index.
				continue
			}
			s.pendingHourEntriesLock.Lock()
			s.pendingHourEntries = append(s.pendingHourEntries, traceID)
			s.pendingHourEntriesLock.Unlock()
		}

		// Slower path: check global cache for (date, traceID) entry.
		if traceID == prevTraceID && hour == prevHour {
			// Fast path for bulk import of multiple rows with the same (date, traceID) pairs.
			continue
		}
		prevHour = hour
		prevTraceID = traceID
		pendingHourSpans = append(pendingHourSpans, pendingSpan{
			timestamp: timestamp,
			traceID:   traceID,
			metricID:  metricID,
		})
	}

	if len(pendingHourSpans) == 0 {
		// Fast path - there are no new (date, metricID) entires in rows.
		return nil
	}

	// Slow path - add new (date, metricID) entries to indexDB.

	atomic.AddUint64(&s.slowPerDayIndexInserts, uint64(len(pendingHourSpans)))

	sort.Slice(pendingHourSpans, func(i, j int) bool {
		return pendingHourSpans[i].traceID.Less(pendingHourSpans[j].traceID)
	})

	idb := s.idb()
	is := idb.getIndexSearch(noDeadline)
	defer idb.putIndexSearch(is)
	var firstError error
	var prevMetricID uint64
	prevTraceID.Reset()
	for _, sp := range pendingHourSpans {
		if sp.traceID == prevTraceID && sp.metricID == prevMetricID {
			// Fast path for bulk import of multiple rows with the same (traceID, metricID) pairs.
			continue
		}
		prevTraceID = sp.traceID
		prevMetricID = sp.metricID

		if err := is.createIndexes(sp.traceID, sp.metricID, sp.timestamp); err != nil {
			if firstError == nil {
				firstError = fmt.Errorf("error when storing (traceID=%s, metricID=%d) in database: %w", sp.traceID, sp.metricID, err)
			}
			continue
		}
	}
	return firstError
}

func (s *Storage) updateCurrHourTraceIDs() {
	hm := s.currHourTraceIDs.Load().(*hourTraceIDs)
	s.pendingHourEntriesLock.Lock()
	newEntries := append([]TraceID{}, s.pendingHourEntries...)
	s.pendingHourEntries = s.pendingHourEntries[:0]
	s.pendingHourEntriesLock.Unlock()
	hour := fasttime.UnixHour()
	if len(newEntries) == 0 && hm.hour == hour {
		// Fast path: nothing to update.
		return
	}

	// Slow path: hm.m must be updated with non-empty s.pendingHourEntries.
	var m *uint128.Set
	isFull := hm.isFull
	if hm.hour == hour {
		m = hm.m.Clone()
	} else {
		m = &uint128.Set{}
		isFull = true
	}

	for _, x := range newEntries {
		m.Add(x)
	}

	hmNew := &hourTraceIDs{
		m:      m,
		hour:   hour,
		isFull: isFull,
	}
	s.currHourTraceIDs.Store(hmNew)
	if hm.hour != hour {
		s.prevHourTraceIDs.Store(hm)
	}
}

type hourTraceIDs struct {
	m      *uint128.Set
	hour   uint64
	isFull bool
}

func openIndexDBTables(path string, metricIDCache, metricNameCache *workingsetcache.Cache) (curr, prev *indexDB, err error) {
	if err := fs.MkdirAllIfNotExist(path); err != nil {
		return nil, nil, fmt.Errorf("cannot create directory %q: %w", path, err)
	}

	d, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot open directory: %w", err)
	}
	defer fs.MustClose(d)

	// Search for the two most recent tables - the last one is active,
	// the previous one contains backup data.
	fis, err := d.Readdir(-1)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot read directory: %w", err)
	}
	var tableNames []string
	for _, fi := range fis {
		if !fs.IsDirOrSymlink(fi) {
			// Skip non-directories.
			continue
		}
		tableName := fi.Name()
		if !indexDBTableNameRegexp.MatchString(tableName) {
			// Skip invalid directories.
			continue
		}
		tableNames = append(tableNames, tableName)
	}
	sort.Slice(tableNames, func(i, j int) bool {
		return tableNames[i] < tableNames[j]
	})
	if len(tableNames) < 2 {
		// Create missing tables
		if len(tableNames) == 0 {
			prevName := nextIndexDBTableName()
			tableNames = append(tableNames, prevName)
		}
		currName := nextIndexDBTableName()
		tableNames = append(tableNames, currName)
	}

	// Invariant: len(tableNames) >= 2

	// Remove all the tables except two last tables.
	for _, tn := range tableNames[:len(tableNames)-2] {
		pathToRemove := path + "/" + tn
		logger.Infof("removing obsolete indexdb dir %q...", pathToRemove)
		fs.MustRemoveAll(pathToRemove)
		logger.Infof("removed obsolete indexdb dir %q", pathToRemove)
	}

	// Persist changes on the file system.
	fs.MustSyncPath(path)

	// Open the last two tables.
	currPath := path + "/" + tableNames[len(tableNames)-1]

	curr, err = openIndexDB(currPath, metricIDCache, metricNameCache)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot open curr indexdb table at %q: %w", currPath, err)
	}
	prevPath := path + "/" + tableNames[len(tableNames)-2]
	prev, err = openIndexDB(prevPath, metricIDCache, metricNameCache)
	if err != nil {
		curr.MustClose()
		return nil, nil, fmt.Errorf("cannot open prev indexdb table at %q: %w", prevPath, err)
	}

	return curr, prev, nil
}

var indexDBTableNameRegexp = regexp.MustCompile("^[0-9A-F]{16}$")

func nextIndexDBTableName() string {
	n := atomic.AddUint64(&indexDBTableIdx, 1)
	return fmt.Sprintf("%016X", n)
}

var indexDBTableIdx = uint64(time.Now().UnixNano())
