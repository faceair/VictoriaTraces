package storage

import (
	"bytes"
	"fmt"
	"io"
	"sort"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/uint64set"
	"github.com/faceair/VictoriaTraces/lib/uint128"
)

func (is *indexSearch) createIndexes(traceID TraceID, metricID, timestamp uint64) error {
	items := getIndexItems()
	defer putIndexItems(items)

	// Create per-day inverted index entries for metricID.
	kb := kbPool.Get()
	defer kbPool.Put(kb)

	mn := GetSpanName()
	defer PutSpanName(mn)
	var err error

	// There is no need in searching for metric name in is.db.extDB,
	// Since the storeDateMetricID function is called only after the metricID->metricName
	// is added into the current is.db.
	kb.B, err = is.searchMetricName(kb.B[:0], metricID)
	if err != nil {
		if err == io.EOF {
			logger.Errorf("missing metricName by metricID %d; this could be the case after unclean shutdown;", metricID)
			return nil
		}
		return fmt.Errorf("cannot find metricName by metricID %d: %w", metricID, err)
	}
	if err = mn.unmarshalRaw(kb.B); err != nil {
		return fmt.Errorf("cannot unmarshal metricName %q obtained by metricID %d: %w", metricID, kb.B, err)
	}

	for i := range mn.Tags {
		tag := &mn.Tags[i]

		items.B = is.marshalCommonPrefix(items.B, nsPrefixTagTimeToTraceIDs)
		items.B = tag.Marshal(items.B)
		items.B = encoding.MarshalUint64(items.B, timestamp)
		items.B = traceID.Marshal(items.B)
		items.Next()

		items.B = is.marshalCommonPrefix(items.B, nsPrefixTraceIDToTag)
		items.B = traceID.Marshal(items.B)
		items.B = tag.Marshal(items.B)
		items.Next()
	}
	if err = is.db.tb.AddItems(items.Items); err != nil {
		return fmt.Errorf("cannot add per-day entires for metricID %d: %w", metricID, err)
	}
	return nil
}

// GetOrCreateMetricID return metricID for the given metricName.
func (is *indexSearch) GetOrCreateMetricID(metricName []byte) (uint64, error) {
	var err error
	var metricID uint64

	// A hack: skip searching for the TraceID after many serial misses.
	// This should improve insertion performance for big batches
	// of new time series.
	if is.tsidByNameMisses < 100 {
		metricID, err = is.getMetricIDByMetricName(metricName)
		if err == nil {
			is.tsidByNameMisses = 0
			return metricID, nil
		}
		if err != io.EOF {
			return 0, fmt.Errorf("cannot search MetricID by MetricName %q: %w", metricName, err)
		}
		is.tsidByNameMisses++
	} else {
		is.tsidByNameSkips++
		if is.tsidByNameSkips > 10000 {
			is.tsidByNameSkips = 0
			is.tsidByNameMisses = 0
		}
	}

	// MetricID for the given name wasn't found. Create it.
	// It is OK if duplicate MetricID for mn is created by concurrent goroutines.
	// Metric results will be merged by mn after TableSearch.
	if metricID, err = is.db.createMetricIDByName(metricName); err != nil {
		return 0, fmt.Errorf("cannot create MetricID by MetricName %q: %w", metricName, err)
	}
	return metricID, nil
}

func (is *indexSearch) searchMetricName(dst []byte, metricID uint64) ([]byte, error) {
	metricName := is.db.getMetricNameFromCache(dst, metricID)
	if len(metricName) > len(dst) {
		return metricName, nil
	}

	ts := &is.ts
	kb := &is.kb
	kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefixMetricIDToMetricName)
	kb.B = encoding.MarshalUint64(kb.B, metricID)
	kb.B = append(kb.B, kvSeparatorChar)
	if err := ts.FirstItemWithPrefix(kb.B); err != nil {
		if err == io.EOF {
			return dst, err
		}
		return dst, fmt.Errorf("error when searching metricName by metricID; searchPrefix %q: %w", kb.B, err)
	}
	v := ts.Item[len(kb.B):]
	dst = append(dst, v...)

	// There is no need in verifying whether the given metricID is deleted,
	// since the filtering must be performed before calling this func.
	is.db.putMetricToCache(metricID, dst)
	return dst, nil
}

func (is *indexSearch) searchTagValues(tvs map[string]struct{}, tagKey []byte, maxTagValues int) error {
	ts := &is.ts
	kb := &is.kb
	mp := &is.mp
	mp.Reset()
	loopsPaceLimiter := 0
	kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefixTagKeyValue)
	kb.B = marshalTagValue(kb.B, tagKey)
	prefix := kb.B
	ts.Seek(prefix)
	for len(tvs) < maxTagValues && ts.NextItem() {
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

		// Store tag value
		tvs[string(mp.Tag.Value)] = struct{}{}

		if mp.TraceIDsLen() < maxTraceIDsPerRow/2 {
			// There is no need in searching for the next tag value,
			// since it is likely it is located in the next row,
			// because the current row contains incomplete metricIDs set.
			continue
		}
		// Search for the next tag value.
		// The last char in kb.B must be tagSeparatorChar.
		// Just increment it in order to jump to the next tag value.
		kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefixTagKeyValue)
		kb.B = marshalTagValue(kb.B, mp.Tag.Key)
		kb.B = marshalTagValue(kb.B, mp.Tag.Value)
		kb.B[len(kb.B)-1]++
		ts.Seek(kb.B)
	}
	if err := ts.Error(); err != nil {
		return fmt.Errorf("error when searching for tag name prefix %q: %w", prefix, err)
	}
	return nil
}

func (is *indexSearch) getMetricIDByMetricName(metricName []byte) (uint64, error) {
	ts := &is.ts
	kb := &is.kb
	kb.B = append(kb.B[:0], nsPrefixMetricNameToMetricID)
	kb.B = append(kb.B, metricName...)
	kb.B = append(kb.B, kvSeparatorChar)
	ts.Seek(kb.B)
	for ts.NextItem() {
		if !bytes.HasPrefix(ts.Item, kb.B) {
			// Nothing found.
			return 0, io.EOF
		}
		tail := ts.Item[len(kb.B):]
		if len(tail) != 8 {
			return 0, fmt.Errorf("unexpected non-empty tail left after unmarshaling TraceID: %X", tail)
		}
		metricID := encoding.UnmarshalUint64(tail)
		return metricID, nil
	}
	if err := ts.Error(); err != nil {
		return 0, fmt.Errorf("error when searching TraceID by metricName; searchPrefix %q: %w", kb.B, err)
	}
	// Nothing found
	return 0, io.EOF
}

// updateMetricIDsByMetricNameMatch matches metricName values for the given srcMetricIDs against tfs
// and adds matching metrics to metricIDs.
func (is *indexSearch) updateMetricIDsByMetricNameMatch(metricIDs, srcMetricIDs *uint64set.Set, tfs []*tagFilter) error {
	// sort srcMetricIDs in order to speed up Seek below.
	sortedMetricIDs := srcMetricIDs.AppendTo(nil)

	metricName := kbPool.Get()
	defer kbPool.Put(metricName)
	mn := GetSpanName()
	defer PutSpanName(mn)
	for loopsPaceLimiter, metricID := range sortedMetricIDs {
		if loopsPaceLimiter&paceLimiterSlowIterationsMask == 0 {
			if err := checkSearchDeadlineAndPace(is.deadline); err != nil {
				return err
			}
		}
		var err error
		metricName.B, err = is.searchMetricName(metricName.B[:0], metricID)
		if err != nil {
			if err == io.EOF {
				// It is likely the metricID->metricName entry didn't propagate to inverted index yet.
				// Skip this metricID for now.
				continue
			}
			return fmt.Errorf("cannot find metricName by metricID %d: %w", metricID, err)
		}
		if err := mn.unmarshalRaw(metricName.B); err != nil {
			return fmt.Errorf("cannot unmarshal metricName %q: %w", metricName.B, err)
		}

		// Match the mn against tfs.
		ok, err := matchTagFilters(mn, tfs, &is.kb)
		if err != nil {
			return fmt.Errorf("cannot match SpanName %s against tagFilters: %w", mn, err)
		}
		if !ok {
			continue
		}
		metricIDs.Add(metricID)
	}
	return nil
}

func (is *indexSearch) searchTraceIDs(tfss []*TagFilters, tr ScanRange) ([]TraceID, error) {
	traceIDs := &uint128.Set{}
	for _, tfs := range tfss {
		for i := range tfs.tfs {
			tf := &tfs.tfs[i]
			if len(tf.key) == 0 && !tf.isNegative && !tf.isRegexp {
				traceID, err := parseTraceID(bytesutil.ToUnsafeString(tf.value))
				if err != nil {
					return nil, err
				}
				traceIDs.Add(traceID)
				break
			}
		}
		m, err := is.getTraceIDsByScanRange(tfs, tr)
		if err != nil {
			return nil, err
		}
		traceIDs.UnionMayOwn(m)
	}
	if traceIDs.Len() == 0 {
		// Nothing found
		return nil, nil
	}
	return traceIDs.AppendTo(nil), nil
}

func (is *indexSearch) getTraceIDsByScanRange(tfs *TagFilters, sr ScanRange) (*uint128.Set, error) {
	// Sort tfs by the number of matching filters from previous queries.
	// This way we limit the amount of work below by applying more specific filters at first.
	type tagFilterWithCount struct {
		tf   *tagFilter
		cost uint64
	}
	date := uint64(sr.MaxTimestamp) / msecPerDay
	tfsWithCount := make([]tagFilterWithCount, len(tfs.tfs))
	kb := &is.kb
	var buf []byte
	for i := range tfs.tfs {
		tf := &tfs.tfs[i]
		kb.B = appendDateTagFilterCacheKey(kb.B[:0], date, tf)
		buf = is.db.traceIDsPerDateTagFilterCache.Get(buf[:0], kb.B)
		count := uint64(0)
		if len(buf) == 8 {
			count = encoding.UnmarshalUint64(buf)
		}
		tfsWithCount[i] = tagFilterWithCount{
			tf:   tf,
			cost: count * tf.matchCost,
		}
	}
	sort.Slice(tfsWithCount, func(i, j int) bool {
		a, b := &tfsWithCount[i], &tfsWithCount[j]
		if a.cost != b.cost {
			return a.cost < b.cost
		}
		return a.tf.Less(b.tf)
	})

	// Populate traceIDs with the first non-negative filter.
	var traceIDs *uint128.Set
	maxLimit := sr.Limit * 100
	minTimestamp := uint64(sr.MinTimestamp)
	maxTimestamp := uint64(sr.MaxTimestamp)
	tfsRemainingWithCount := tfsWithCount[:0]
	for i := range tfsWithCount {
		tf := tfsWithCount[i].tf
		if tf.isNegative {
			tfsRemainingWithCount = append(tfsRemainingWithCount, tfsWithCount[i])
			continue
		}
		m, err := is.getTraceIDsByTimeRangeTagFilter(tf, tfs.commonPrefix, minTimestamp, maxTimestamp, maxLimit)
		if err != nil {
			return nil, err
		}
		traceIDs = m
		i++
		for i < len(tfsWithCount) {
			tfsRemainingWithCount = append(tfsRemainingWithCount, tfsWithCount[i])
			i++
		}
		break
	}
	if traceIDs == nil || traceIDs.Len() == 0 {
		// There is no sense in inspecting tfsRemainingWithCount, since the result will be empty.
		return nil, nil
	}

	// Intersect traceIDs with the rest of filters.
	for i := range tfsRemainingWithCount {
		tfWithCount := tfsRemainingWithCount[i]
		tf := tfWithCount.tf
		m, err := is.getTraceIDsByTimeRangeTagFilter(tf, tfs.commonPrefix, minTimestamp, maxTimestamp, maxLimit)
		if err != nil {
			return nil, err
		}
		if tf.isNegative {
			traceIDs.Subtract(m)
		} else {
			traceIDs.Intersect(m)
		}
		if traceIDs.Len() == 0 {
			// Short circuit - there is no need in applying the remaining filters to empty set.
			return nil, nil
		}
	}
	return traceIDs, nil
}

func (is *indexSearch) getTraceIDsByTimeRangeTagFilter(tf *tagFilter, commonPrefix []byte, minTimestamp, maxTimestamp uint64, limit int) (*uint128.Set, error) {
	// Augument tag filter prefix for per-minute search instead of global search.
	if !bytes.HasPrefix(tf.prefix, commonPrefix) {
		logger.Panicf("BUG: unexpected tf.prefix %q; must start with commonPrefix %q", tf.prefix, commonPrefix)
	}
	kb := kbPool.Get()
	defer kbPool.Put(kb)
	kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefixTagTimeToTraceIDs)
	kb.B = append(kb.B, tf.prefix[len(commonPrefix):]...)

	tfNew := *tf
	tfNew.isNegative = false // isNegative for the original tf is handled by the caller.
	tfNew.prefix = kb.B
	return is.getTraceIDsByTimeRangeForTagFilter(&tfNew, minTimestamp, maxTimestamp, limit)
}

func (is *indexSearch) getTraceIDsByTimeRangeForTagFilter(tf *tagFilter, minTimestamp, maxTimestamp uint64, limit int) (*uint128.Set, error) {
	if tf.isNegative {
		logger.Panicf("BUG: isNegative must be false")
	}
	if len(tf.orSuffixes) != 1 && tf.orSuffixes[0] != "" {
		return nil, fmt.Errorf("can't find metricid by range with %s", tf)
	}

	traceIDs := &uint128.Set{}
	if err := is.updateTraceIDsByTimeRangeForOrSuffixesNoFilter(traceIDs, tf, minTimestamp, maxTimestamp, limit); err != nil {
		return nil, fmt.Errorf("error when searching for metricIDs for tagFilter in fast path: %w; tagFilter=%s", err, tf)
	}

	date := maxTimestamp / msecPerDay
	// Store the number of matching metricIDs in the cache in order to sort tag filters
	// in ascending number of matching metricIDs on the next search.
	is.kb.B = appendDateTagFilterCacheKey(is.kb.B[:0], date, tf)
	traceIDsLen := uint64(traceIDs.Len())
	is.kb.B = encoding.MarshalUint64(is.kb.B[:0], traceIDsLen)
	is.db.traceIDsPerDateTagFilterCache.Set(is.kb.B, is.kb.B)

	return traceIDs, nil
}

func (is *indexSearch) updateTraceIDsByTimeRangeForOrSuffixesNoFilter(traceIDs *uint128.Set, tf *tagFilter, minTimestamp, maxTimestamp uint64, limit int) error {
	if tf.isNegative {
		logger.Panicf("BUG: isNegative must be false")
	}

	startKB := kbPool.Get()
	defer kbPool.Put(startKB)

	endKB := kbPool.Get()
	defer kbPool.Put(endKB)

	for _, orSuffix := range tf.orSuffixes {
		startKB.B = append(startKB.B[:0], tf.prefix...)
		startKB.B = append(startKB.B, orSuffix...)
		startKB.B = append(startKB.B, tagSeparatorChar)
		startKB.B = encoding.MarshalUint64(startKB.B, minTimestamp)

		endKB.B = append(endKB.B[:0], tf.prefix...)
		endKB.B = append(endKB.B, orSuffix...)
		endKB.B = append(endKB.B, tagSeparatorChar)
		endKB.B = encoding.MarshalUint64(endKB.B, maxTimestamp)

		if err := is.updateTraceIDsByPrefixRange(traceIDs, startKB.B, endKB.B, limit); err != nil {
			return err
		}
		if traceIDs.Len() >= limit {
			return nil
		}
	}
	return nil
}

func (is *indexSearch) updateTraceIDsByPrefixRange(traceIDs *uint128.Set, startPrefix, endPrefix []byte, limit int) error {
	ts := &is.ts
	mp := &is.mp
	mp.Reset()
	loopsPaceLimiter := 0
	ts.Seek(startPrefix)
	for traceIDs.Len() < limit && ts.NextItem() {
		if loopsPaceLimiter&paceLimiterFastIterationsMask == 0 {
			if err := checkSearchDeadlineAndPace(is.deadline); err != nil {
				return err
			}
		}
		loopsPaceLimiter++
		item := ts.Item
		if string(item) > string(endPrefix) {
			return nil
		}
		if err := mp.InitOnlyTail(item, item[len(startPrefix):]); err != nil {
			return err
		}
		mp.ParseTraceIDs()
		remain := limit - traceIDs.Len()
		if len(mp.TraceIDs) > remain {
			traceIDs.AddMulti(mp.TraceIDs[:remain])
			return nil
		}
		traceIDs.AddMulti(mp.TraceIDs)
	}
	if err := ts.Error(); err != nil {
		return fmt.Errorf("error when searching for tag filter prefix %q-%q: %w", startPrefix, endPrefix, err)
	}
	return nil
}
