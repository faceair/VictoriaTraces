package vmselect

import (
	"container/heap"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/searchutils"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/handshake"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/netutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/syncwg"
	"github.com/VictoriaMetrics/metrics"
	"github.com/faceair/VictoriaTraces/lib/storage"
)

// Result is a single timeseries result.
//
// ProcessSearchQuery returns Result slice.
type Result struct {
	// The name of the metric.
	SpanName storage.SpanName

	// Values are sorted by Timestamps.
	Values     [][]byte
	Timestamps []int64

	// Marshaled SpanName. Used only for results sorting
	// in app/vmselect/promql
	MetricNameMarshaled []byte
}

func (r *Result) reset() {
	r.SpanName.Reset()
	r.Values = r.Values[:0]
	r.Timestamps = r.Timestamps[:0]
	r.MetricNameMarshaled = r.MetricNameMarshaled[:0]
}

// Results holds results returned from ProcessSearchQuery.
type Results struct {
	sr        storage.ScanRange
	fetchData bool
	deadline  searchutils.Deadline

	tbf *tmpBlocksFile

	packedTimeseries []packedTimeseries
}

// Len returns the number of results in rss.
func (rss *Results) Len() int {
	return len(rss.packedTimeseries)
}

// Cancel cancels rss work.
func (rss *Results) Cancel() {
	putTmpBlocksFile(rss.tbf)
	rss.tbf = nil
}

var timeseriesWorkCh = make(chan *timeseriesWork, gomaxprocs*16)

type timeseriesWork struct {
	mustStop uint64
	rss      *Results
	pts      *packedTimeseries
	f        func(rs *Result, workerID uint) error
	doneCh   chan error

	rowsProcessed int
}

func init() {
	for i := 0; i < gomaxprocs; i++ {
		go timeseriesWorker(uint(i))
	}
}

func timeseriesWorker(workerID uint) {
	var rs Result
	var rsLastResetTime uint64
	for tsw := range timeseriesWorkCh {
		rss := tsw.rss
		if rss.deadline.Exceeded() {
			tsw.doneCh <- fmt.Errorf("timeout exceeded during query execution: %s", rss.deadline.String())
			continue
		}
		if atomic.LoadUint64(&tsw.mustStop) != 0 {
			tsw.doneCh <- nil
			continue
		}
		if err := tsw.pts.Unpack(rss.tbf, &rs, rss.sr, rss.fetchData); err != nil {
			tsw.doneCh <- fmt.Errorf("error during time series unpacking: %w", err)
			continue
		}
		if len(rs.Timestamps) > 0 || !rss.fetchData {
			if err := tsw.f(&rs, workerID); err != nil {
				tsw.doneCh <- err
				continue
			}
		}
		tsw.rowsProcessed = len(rs.Values)
		tsw.doneCh <- nil
		currentTime := fasttime.UnixTimestamp()
		if cap(rs.Values) > 1024*1024 && 4*len(rs.Values) < cap(rs.Values) && currentTime-rsLastResetTime > 10 {
			// Reset rs in order to preseve memory usage after processing big time series with millions of rows.
			rs = Result{}
			rsLastResetTime = currentTime
		}
	}
}

// RunParallel runs f in parallel for all the results from rss.
//
// f shouldn't hold references to rs after returning.
// workerID is the id of the worker goroutine that calls f.
// Data processing is immediately stopped if f returns non-nil error.
//
// rss becomes unusable after the call to RunParallel.
func (rss *Results) RunParallel(f func(rs *Result, workerID uint) error) error {
	defer func() {
		putTmpBlocksFile(rss.tbf)
		rss.tbf = nil
	}()

	// Feed workers with work.
	tsws := make([]*timeseriesWork, len(rss.packedTimeseries))
	for i := range rss.packedTimeseries {
		tsw := &timeseriesWork{
			rss:    rss,
			pts:    &rss.packedTimeseries[i],
			f:      f,
			doneCh: make(chan error, 1),
		}
		timeseriesWorkCh <- tsw
		tsws[i] = tsw
	}
	seriesProcessedTotal := len(rss.packedTimeseries)
	rss.packedTimeseries = rss.packedTimeseries[:0]

	// Wait until work is complete.
	var firstErr error
	rowsProcessedTotal := 0
	for _, tsw := range tsws {
		if err := <-tsw.doneCh; err != nil && firstErr == nil {
			// Return just the first error, since other errors
			// are likely duplicate the first error.
			firstErr = err
			// Notify all the the tsws that they shouldn't be executed.
			for _, tsw := range tsws {
				atomic.StoreUint64(&tsw.mustStop, 1)
			}
		}
		rowsProcessedTotal += tsw.rowsProcessed
	}

	perQueryRowsProcessed.Update(float64(rowsProcessedTotal))
	perQuerySeriesProcessed.Update(float64(seriesProcessedTotal))
	return firstErr
}

var perQueryRowsProcessed = metrics.NewHistogram(`vm_per_query_rows_processed_count`)
var perQuerySeriesProcessed = metrics.NewHistogram(`vm_per_query_series_processed_count`)

var gomaxprocs = cgroup.AvailableCPUs()

type packedTimeseries struct {
	metricName string
	addrs      []tmpBlockAddr
}

var unpackWorkCh = make(chan *unpackWork, gomaxprocs*128)

type unpackWorkItem struct {
	addr tmpBlockAddr
	sr   storage.ScanRange
}

type unpackWork struct {
	ws     []unpackWorkItem
	tbf    *tmpBlocksFile
	sbs    []*sortBlock
	doneCh chan error
}

func (upw *unpackWork) reset() {
	ws := upw.ws
	for i := range ws {
		w := &ws[i]
		w.addr = tmpBlockAddr{}
		w.sr = storage.ScanRange{}
	}
	upw.ws = upw.ws[:0]
	upw.tbf = nil
	sbs := upw.sbs
	for i := range sbs {
		sbs[i] = nil
	}
	upw.sbs = upw.sbs[:0]
	if n := len(upw.doneCh); n > 0 {
		logger.Panicf("BUG: upw.doneCh must be empty; it contains %d items now", n)
	}
}

func (upw *unpackWork) unpack(tmpBlock *storage.Block) {
	for _, w := range upw.ws {
		sb := getSortBlock()
		if err := sb.unpackFrom(tmpBlock, upw.tbf, w.addr, w.sr); err != nil {
			putSortBlock(sb)
			upw.doneCh <- fmt.Errorf("cannot unpack block: %w", err)
			return
		}
		upw.sbs = append(upw.sbs, sb)
	}
	upw.doneCh <- nil
}

func getUnpackWork() *unpackWork {
	v := unpackWorkPool.Get()
	if v != nil {
		return v.(*unpackWork)
	}
	return &unpackWork{
		doneCh: make(chan error, 1),
	}
}

func putUnpackWork(upw *unpackWork) {
	upw.reset()
	unpackWorkPool.Put(upw)
}

var unpackWorkPool sync.Pool

func init() {
	for i := 0; i < gomaxprocs; i++ {
		go unpackWorker()
	}
}

func unpackWorker() {
	var tmpBlock storage.Block
	for upw := range unpackWorkCh {
		upw.unpack(&tmpBlock)
	}
}

// unpackBatchSize is the maximum number of blocks that may be unpacked at once by a single goroutine.
//
// This batch is needed in order to reduce contention for upackWorkCh in multi-CPU system.
var unpackBatchSize = 8 * cgroup.AvailableCPUs()

// Unpack unpacks pts to dst.
func (pts *packedTimeseries) Unpack(tbf *tmpBlocksFile, dst *Result, sr storage.ScanRange, fetchData bool) error {
	dst.reset()
	if err := dst.SpanName.Unmarshal(bytesutil.ToUnsafeBytes(pts.metricName)); err != nil {
		return fmt.Errorf("cannot unmarshal metricName %q: %w", pts.metricName, err)
	}
	if !fetchData {
		// Do not spend resources on data reading and unpacking.
		return nil
	}

	// Feed workers with work
	addrsLen := len(pts.addrs)
	upws := make([]*unpackWork, 0, 1+addrsLen/unpackBatchSize)
	upw := getUnpackWork()
	upw.tbf = tbf
	for _, addr := range pts.addrs {
		if len(upw.ws) >= unpackBatchSize {
			unpackWorkCh <- upw
			upws = append(upws, upw)
			upw = getUnpackWork()
			upw.tbf = tbf
		}
		upw.ws = append(upw.ws, unpackWorkItem{
			addr: addr,
			sr:   sr,
		})
	}
	unpackWorkCh <- upw
	upws = append(upws, upw)
	pts.addrs = pts.addrs[:0]

	// Wait until work is complete
	sbs := make([]*sortBlock, 0, addrsLen)
	var firstErr error
	for _, upw := range upws {
		if err := <-upw.doneCh; err != nil && firstErr == nil {
			// Return the first error only, since other errors are likely the same.
			firstErr = err
		}
		if firstErr == nil {
			sbs = append(sbs, upw.sbs...)
		} else {
			for _, sb := range upw.sbs {
				putSortBlock(sb)
			}
		}
		putUnpackWork(upw)
	}
	if firstErr != nil {
		return firstErr
	}
	mergeSortBlocks(dst, sbs)
	return nil
}

func getSortBlock() *sortBlock {
	v := sbPool.Get()
	if v == nil {
		return &sortBlock{}
	}
	return v.(*sortBlock)
}

func putSortBlock(sb *sortBlock) {
	sb.reset()
	sbPool.Put(sb)
}

var sbPool sync.Pool

var metricRowsSkipped = metrics.NewCounter(`vm_metric_rows_skipped_total{name="vmselect"}`)

func mergeSortBlocks(dst *Result, sbh sortBlocksHeap) {
	// Skip empty sort blocks, since they cannot be passed to heap.Init.
	src := sbh
	sbh = sbh[:0]
	for _, sb := range src {
		if len(sb.Timestamps) == 0 {
			putSortBlock(sb)
			continue
		}
		sbh = append(sbh, sb)
	}
	if len(sbh) == 0 {
		return
	}
	heap.Init(&sbh)
	for {
		top := sbh[0]
		heap.Pop(&sbh)
		if len(sbh) == 0 {
			dst.Timestamps = append(dst.Timestamps, top.Timestamps[top.NextIdx:]...)
			dst.Values = append(dst.Values, top.Values[top.NextIdx:]...)
			putSortBlock(top)
			break
		}
		sbNext := sbh[0]
		tsNext := sbNext.Timestamps[sbNext.NextIdx]
		idxNext := len(top.Timestamps)
		if top.Timestamps[idxNext-1] > tsNext {
			idxNext = top.NextIdx
			for top.Timestamps[idxNext] <= tsNext {
				idxNext++
			}
		}
		dst.Timestamps = append(dst.Timestamps, top.Timestamps[top.NextIdx:idxNext]...)
		dst.Values = append(dst.Values, top.Values[top.NextIdx:idxNext]...)
		if idxNext < len(top.Timestamps) {
			top.NextIdx = idxNext
			heap.Push(&sbh, top)
		} else {
			// Return top to the pool.
			putSortBlock(top)
		}
	}
}

type sortBlock struct {
	Timestamps []int64
	Values     [][]byte
	NextIdx    int
}

func (sb *sortBlock) reset() {
	sb.Timestamps = sb.Timestamps[:0]
	sb.Values = sb.Values[:0]
	sb.NextIdx = 0
}

func (sb *sortBlock) unpackFrom(tmpBlock *storage.Block, tbf *tmpBlocksFile, addr tmpBlockAddr, sr storage.ScanRange) error {
	tmpBlock.Reset()
	tbf.MustReadBlockAt(tmpBlock, addr)
	if err := tmpBlock.UnmarshalData(); err != nil {
		return fmt.Errorf("cannot unmarshal block: %w", err)
	}
	sb.Timestamps, sb.Values = tmpBlock.AppendRowsWithTimeRangeFilter(sb.Timestamps[:0], sb.Values[:0], sr)
	skippedRows := tmpBlock.RowsCount() - len(sb.Timestamps)
	metricRowsSkipped.Add(skippedRows)
	return nil
}

type sortBlocksHeap []*sortBlock

func (sbh sortBlocksHeap) Len() int {
	return len(sbh)
}

func (sbh sortBlocksHeap) Less(i, j int) bool {
	a := sbh[i]
	b := sbh[j]
	return a.Timestamps[a.NextIdx] < b.Timestamps[b.NextIdx]
}

func (sbh sortBlocksHeap) Swap(i, j int) {
	sbh[i], sbh[j] = sbh[j], sbh[i]
}

func (sbh *sortBlocksHeap) Push(x interface{}) {
	*sbh = append(*sbh, x.(*sortBlock))
}

func (sbh *sortBlocksHeap) Pop() interface{} {
	a := *sbh
	v := a[len(a)-1]
	*sbh = a[:len(a)-1]
	return v
}

// GetLabelValues returns label values for the given labelName
// until the given deadline.
func GetLabelValues(denyPartialResponse bool, labelName string, deadline searchutils.Deadline) ([]string, bool, error) {
	if deadline.Exceeded() {
		return nil, false, fmt.Errorf("timeout exceeded before starting the query processing: %s", deadline.String())
	}
	if labelName == "__name__" {
		labelName = ""
	}

	// Send the query to all the storage nodes in parallel.
	type nodeResult struct {
		labelValues []string
		err         error
	}
	snr := startStorageNodesRequest(denyPartialResponse, func(idx int, sn *storageNode) interface{} {
		sn.labelValuesRequests.Inc()
		labelValues, err := sn.getLabelValues(labelName, deadline)
		if err != nil {
			sn.labelValuesErrors.Inc()
			err = fmt.Errorf("cannot get label values from vmstorage %s: %w", sn.connPool.Addr(), err)
		}
		return &nodeResult{
			labelValues: labelValues,
			err:         err,
		}
	})

	// Collect results
	var labelValues []string
	isPartial, err := snr.collectResults(partialLabelValuesResults, func(result interface{}) error {
		nr := result.(*nodeResult)
		if nr.err != nil {
			return nr.err
		}
		labelValues = append(labelValues, nr.labelValues...)
		return nil
	})
	if err != nil {
		return nil, isPartial, fmt.Errorf("cannot fetch label values from vmstorage nodes: %w", err)
	}

	// Deduplicate label values
	labelValues = deduplicateStrings(labelValues)
	// Sort labelValues like Prometheus does
	sort.Strings(labelValues)
	return labelValues, isPartial, nil
}

func deduplicateStrings(a []string) []string {
	m := make(map[string]bool, len(a))
	for _, s := range a {
		m[s] = true
	}
	a = a[:0]
	for s := range m {
		a = append(a, s)
	}
	return a
}

type tmpBlocksFileWrapper struct {
	mu                 sync.Mutex
	tbf                *tmpBlocksFile
	m                  map[string][]tmpBlockAddr
	orderedMetricNames []string
}

func (tbfw *tmpBlocksFileWrapper) RegisterEmptyBlock(mb *storage.Block) {
	traceID := mb.TraceID().String()
	tbfw.mu.Lock()
	if addrs := tbfw.m[traceID]; addrs == nil {
		// An optimization for big number of time series with long names: store only a single copy of metricNameStr
		// in both tbfw.orderedMetricNames and tbfw.m.
		tbfw.orderedMetricNames = append(tbfw.orderedMetricNames, traceID)
		tbfw.m[tbfw.orderedMetricNames[len(tbfw.orderedMetricNames)-1]] = []tmpBlockAddr{{}}
	}
	tbfw.mu.Unlock()
}

func (tbfw *tmpBlocksFileWrapper) RegisterAndWriteBlock(mb *storage.Block) error {
	bb := tmpBufPool.Get()
	bb.B = storage.MarshalBlock(bb.B[:0], mb)
	tbfw.mu.Lock()
	addr, err := tbfw.tbf.WriteBlockData(bb.B)
	tmpBufPool.Put(bb)
	if err == nil {
		traceID := mb.TraceID().String()
		addrs := tbfw.m[traceID]
		addrs = append(addrs, addr)
		if len(addrs) > 1 {
			// An optimization: avoid memory allocation and copy for already existing metricName key in tbfw.m.
			tbfw.m[traceID] = addrs
		} else {
			// An optimization for big number of time series with long names: store only a single copy of metricNameStr
			// in both tbfw.orderedMetricNames and tbfw.m.
			tbfw.orderedMetricNames = append(tbfw.orderedMetricNames, traceID)
			tbfw.m[tbfw.orderedMetricNames[len(tbfw.orderedMetricNames)-1]] = addrs
		}
	}
	tbfw.mu.Unlock()
	return err
}

func SearchTraceIDs(denyPartialResponse bool, sq *storage.SearchQuery, deadline searchutils.Deadline) ([]storage.TraceID, bool, error) {
	if deadline.Exceeded() {
		return nil, false, fmt.Errorf("timeout exceeded before starting to search metric names: %s", deadline.String())
	}
	sq.FetchData = storage.NotFetch
	requestData := sq.Marshal(nil)

	// Send the query to all the storage nodes in parallel.
	type nodeResult struct {
		traceIDs []storage.TraceID
		err      error
	}
	snr := startStorageNodesRequest(denyPartialResponse, func(idx int, sn *storageNode) interface{} {
		sn.searchMetricNamesRequests.Inc()
		traceIDs, err := sn.processSearchTraceIDs(requestData, deadline)
		if err != nil {
			sn.searchMetricNamesErrors.Inc()
			err = fmt.Errorf("cannot search metric names on vmstorage %s: %w", sn.connPool.Addr(), err)
		}
		return &nodeResult{
			traceIDs: traceIDs,
			err:      err,
		}
	})

	// Collect results.
	traceIDs := make([]storage.TraceID, 0)
	isPartial, err := snr.collectResults(partialSearchMetricNamesResults, func(result interface{}) error {
		nr := result.(*nodeResult)
		if nr.err != nil {
			return nr.err
		}
		traceIDs = append(traceIDs, nr.traceIDs...)
		return nil
	})
	if err != nil {
		return nil, isPartial, fmt.Errorf("cannot fetch metric names from vmstorage nodes: %w", err)
	}
	return traceIDs, isPartial, nil
}

// ProcessSearchQuery performs sq until the given deadline.
//
// Results.RunParallel or Results.Cancel must be called on the returned Results.
func ProcessSearchQuery(denyPartialResponse bool, sq *storage.SearchQuery, deadline searchutils.Deadline) (*Results, bool, error) {
	if deadline.Exceeded() {
		return nil, false, fmt.Errorf("timeout exceeded before starting the query processing: %s", deadline.String())
	}
	fetchData := sq.FetchData == storage.NotFetch
	tr := storage.ScanRange{
		MinTimestamp: sq.MinTimestamp,
		MaxTimestamp: sq.MaxTimestamp,
	}
	tbfw := &tmpBlocksFileWrapper{
		tbf: getTmpBlocksFile(),
		m:   make(map[string][]tmpBlockAddr),
	}
	var wg syncwg.WaitGroup
	var stopped uint32
	processBlock := func(mb *storage.Block) error {
		wg.Add(1)
		defer wg.Done()
		if atomic.LoadUint32(&stopped) != 0 {
			return nil
		}
		if !fetchData {
			tbfw.RegisterEmptyBlock(mb)
			return nil
		}
		if err := tbfw.RegisterAndWriteBlock(mb); err != nil {
			return fmt.Errorf("cannot write MetricBlock to temporary blocks file: %w", err)
		}
		return nil
	}
	isPartial, err := processSearchQuery(denyPartialResponse, sq, processBlock, deadline)

	// Make sure processBlock isn't called anymore in order to protect from data races.
	atomic.StoreUint32(&stopped, 1)
	wg.Wait()

	if err != nil {
		putTmpBlocksFile(tbfw.tbf)
		return nil, false, fmt.Errorf("error occured during search: %w", err)
	}
	if err := tbfw.tbf.Finalize(); err != nil {
		putTmpBlocksFile(tbfw.tbf)
		return nil, false, fmt.Errorf("cannot finalize temporary blocks file with %d time series: %w", len(tbfw.m), err)
	}

	var rss Results
	rss.sr = tr
	rss.fetchData = fetchData
	rss.deadline = deadline
	rss.tbf = tbfw.tbf
	pts := make([]packedTimeseries, len(tbfw.orderedMetricNames))
	for i, metricName := range tbfw.orderedMetricNames {
		pts[i] = packedTimeseries{
			metricName: metricName,
			addrs:      tbfw.m[metricName],
		}
	}
	rss.packedTimeseries = pts
	return &rss, isPartial, nil
}

func processSearchQuery(denyPartialResponse bool, sq *storage.SearchQuery, processBlock func(mb *storage.Block) error, deadline searchutils.Deadline) (bool, error) {
	requestData := sq.Marshal(nil)

	// Send the query to all the storage nodes in parallel.
	snr := startStorageNodesRequest(denyPartialResponse, func(idx int, sn *storageNode) interface{} {
		sn.searchRequests.Inc()
		err := sn.processSearchQuery(requestData, processBlock, deadline)
		if err != nil {
			sn.searchErrors.Inc()
			err = fmt.Errorf("cannot perform search on vmstorage %s: %w", sn.connPool.Addr(), err)
		}
		return &err
	})

	// Collect results.
	isPartial, err := snr.collectResults(partialSearchResults, func(result interface{}) error {
		errP := result.(*error)
		return *errP
	})
	if err != nil {
		return isPartial, fmt.Errorf("cannot fetch query results from vmstorage nodes: %w", err)
	}
	return isPartial, nil
}

type storageNodesRequest struct {
	denyPartialResponse bool
	resultsCh           chan interface{}
}

func startStorageNodesRequest(denyPartialResponse bool, f func(idx int, sn *storageNode) interface{}) *storageNodesRequest {
	resultsCh := make(chan interface{}, len(storageNodes))
	for idx, sn := range storageNodes {
		go func(idx int, sn *storageNode) {
			result := f(idx, sn)
			resultsCh <- result
		}(idx, sn)
	}
	return &storageNodesRequest{
		denyPartialResponse: denyPartialResponse,
		resultsCh:           resultsCh,
	}
}

func (snr *storageNodesRequest) collectAllResults(f func(result interface{}) error) error {
	var errs []error
	for i := 0; i < len(storageNodes); i++ {
		result := <-snr.resultsCh
		if err := f(result); err != nil {
			errs = append(errs, err)
			continue
		}
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

func (snr *storageNodesRequest) collectResults(partialResultsCounter *metrics.Counter, f func(result interface{}) error) (bool, error) {
	var errs []error
	resultsCollected := 0
	for i := 0; i < len(storageNodes); i++ {
		// There is no need in timer here, since all the goroutines executing the f function
		// passed to startStorageNodesRequest must be finished until the deadline.
		result := <-snr.resultsCh
		if err := f(result); err != nil {
			errs = append(errs, err)
			continue
		}
		resultsCollected++
		if resultsCollected > len(storageNodes) {
			// There is no need in waiting for the remaining results,
			// because the collected results contain all the data according to the given -replicationFactor.
			// This should speed up responses when a part of vmstorage nodes are slow and/or temporarily unavailable.
			// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/711
			//
			// It is expected that cap(snr.resultsCh) == len(storageNodes), otherwise goroutine leak is possible.
			return false, nil
		}
	}
	isPartial := false
	if len(errs) > 0 {
		if len(errs) == len(storageNodes) {
			// All the vmstorage nodes returned error.
			// Return only the first error, since it has no sense in returning all errors.
			return false, errs[0]
		}

		// Return partial results.
		// This allows gracefully degrade vmselect in the case
		// if a part of storageNodes are temporarily unavailable.
		// Do not return the error, since it may spam logs on busy vmselect
		// serving high amounts of requests.
		partialResultsCounter.Inc()
		if snr.denyPartialResponse {
			return true, errs[0]
		}
		isPartial = true
	}
	return isPartial, nil
}

type storageNode struct {
	connPool *netutil.ConnPool

	// The channel for limiting the maximum number of concurrent queries to storageNode.
	concurrentQueriesCh chan struct{}

	// The number of RegisterMetricNames requests to storageNode.
	registerMetricNamesRequests *metrics.Counter

	// The number of RegisterMetricNames request errors to storageNode.
	registerMetricNamesErrors *metrics.Counter

	// The number of DeleteSeries requests to storageNode.
	deleteSeriesRequests *metrics.Counter

	// The number of DeleteSeries request errors to storageNode.
	deleteSeriesErrors *metrics.Counter

	// The number of requests to labels.
	labelsOnTimeRangeRequests *metrics.Counter

	// The number of requests to labels.
	labelsRequests *metrics.Counter

	// The number of errors during requests to labels.
	labelsOnTimeRangeErrors *metrics.Counter

	// The number of errors during requests to labels.
	labelsErrors *metrics.Counter

	// The number of requests to labelValuesOnTimeRange.
	labelValuesOnTimeRangeRequests *metrics.Counter

	// The number of requests to labelValues.
	labelValuesRequests *metrics.Counter

	// The number of errors during requests to labelValuesOnTimeRange.
	labelValuesOnTimeRangeErrors *metrics.Counter

	// The number of errors during requests to labelValues.
	labelValuesErrors *metrics.Counter

	// The number of requests to labelEntries.
	labelEntriesRequests *metrics.Counter

	// The number of errors during requests to labelEntries.
	labelEntriesErrors *metrics.Counter

	// The number of requests to tagValueSuffixes.
	tagValueSuffixesRequests *metrics.Counter

	// The number of errors during requests to tagValueSuffixes.
	tagValueSuffixesErrors *metrics.Counter

	// The number of requests to tsdb status.
	tsdbStatusRequests *metrics.Counter

	// The number of errors during requests to tsdb status.
	tsdbStatusErrors *metrics.Counter

	// The number of requests to seriesCount.
	seriesCountRequests *metrics.Counter

	// The number of errors during requests to seriesCount.
	seriesCountErrors *metrics.Counter

	// The number of 'search metric names' requests to storageNode.
	searchMetricNamesRequests *metrics.Counter

	// The number of search requests to storageNode.
	searchRequests *metrics.Counter

	// The number of 'search metric names' errors to storageNode.
	searchMetricNamesErrors *metrics.Counter

	// The number of search request errors to storageNode.
	searchErrors *metrics.Counter

	// The number of metric blocks read.
	metricBlocksRead *metrics.Counter

	// The number of read metric rows.
	metricRowsRead *metrics.Counter
}

func (sn *storageNode) getLabelValues(labelName string, deadline searchutils.Deadline) ([]string, error) {
	var labelValues []string
	f := func(bc *handshake.BufferedConn) error {
		lvs, err := sn.getLabelValuesOnConn(bc, labelName)
		if err != nil {
			return err
		}
		labelValues = lvs
		return nil
	}
	if err := sn.execOnConn("labelValues_v1", f, deadline); err != nil {
		// Try again before giving up.
		labelValues = nil
		if err = sn.execOnConn("labelValues_v1", f, deadline); err != nil {
			return nil, err
		}
	}
	return labelValues, nil
}

func (sn *storageNode) processSearchTraceIDs(requestData []byte, deadline searchutils.Deadline) ([]storage.TraceID, error) {
	var traceIDs []storage.TraceID
	f := func(bc *handshake.BufferedConn) error {
		mns, err := sn.processSearchTraceIDsOnConn(bc, requestData)
		if err != nil {
			return err
		}
		traceIDs = mns
		return nil
	}
	if err := sn.execOnConn("searchTraceIDs_v1", f, deadline); err != nil {
		// Try again before giving up.
		if err = sn.execOnConn("searchTraceIDs_v1", f, deadline); err != nil {
			return nil, err
		}
	}
	return traceIDs, nil
}

func (sn *storageNode) processSearchQuery(requestData []byte, processBlock func(mb *storage.Block) error, deadline searchutils.Deadline) error {
	var blocksRead int
	f := func(bc *handshake.BufferedConn) error {
		n, err := sn.processSearchQueryOnConn(bc, requestData, processBlock)
		if err != nil {
			return err
		}
		blocksRead = n
		return nil
	}
	if err := sn.execOnConn("search_v1", f, deadline); err != nil && blocksRead == 0 {
		// Try again before giving up if zero blocks read on the previous attempt.
		if err = sn.execOnConn("search_v1", f, deadline); err != nil {
			return err
		}
	}
	return nil
}

func (sn *storageNode) execOnConn(rpcName string, f func(bc *handshake.BufferedConn) error, deadline searchutils.Deadline) error {
	select {
	case sn.concurrentQueriesCh <- struct{}{}:
	default:
		return fmt.Errorf("too many concurrent queries (more than %d)", cap(sn.concurrentQueriesCh))
	}
	defer func() {
		<-sn.concurrentQueriesCh
	}()

	d := time.Unix(int64(deadline.Deadline()), 0)
	nowSecs := fasttime.UnixTimestamp()
	currentTime := time.Unix(int64(nowSecs), 0)
	storageTimeout := *searchutils.StorageTimeout
	if storageTimeout > 0 {
		dd := currentTime.Add(storageTimeout)
		if dd.Sub(d) < 0 {
			// Limit the remote deadline to storageTimeout,
			// so slow vmstorage nodes may stop processing the request.
			// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/711 .
			// The local deadline remains the same, so data obtained from
			// the remaining vmstorage nodes could be processed locally.
			d = dd
		}
	}
	timeout := d.Sub(currentTime)
	if timeout <= 0 {
		return fmt.Errorf("request timeout reached: %s or -search.storageTimeout=%s", deadline.String(), storageTimeout.String())
	}
	bc, err := sn.connPool.Get()
	if err != nil {
		return fmt.Errorf("cannot obtain connection from a pool: %w", err)
	}
	// Extend the connection deadline by 2 seconds, so the remote storage could return `timeout` error
	// without the need to break the connection.
	connDeadline := d.Add(2 * time.Second)
	if err := bc.SetDeadline(connDeadline); err != nil {
		_ = bc.Close()
		logger.Panicf("FATAL: cannot set connection deadline: %s", err)
	}
	if err := writeBytes(bc, []byte(rpcName)); err != nil {
		// Close the connection instead of returning it to the pool,
		// since it may be broken.
		_ = bc.Close()
		return fmt.Errorf("cannot send rpcName=%q to the server: %w", rpcName, err)
	}

	// Send the remaining timeout instead of deadline to remote server, since it may have different time.
	timeoutSecs := uint32(timeout.Seconds() + 1)
	if err := writeUint32(bc, timeoutSecs); err != nil {
		// Close the connection instead of returning it to the pool,
		// since it may be broken.
		_ = bc.Close()
		return fmt.Errorf("cannot send timeout=%d for rpcName=%q to the server: %w", timeout, rpcName, err)
	}

	if err := f(bc); err != nil {
		remoteAddr := bc.RemoteAddr()
		var er *errRemote
		if errors.As(err, &er) {
			// Remote error. The connection may be re-used. Return it to the pool.
			sn.connPool.Put(bc)
		} else {
			// Local error.
			// Close the connection instead of returning it to the pool,
			// since it may be broken.
			_ = bc.Close()
		}
		return fmt.Errorf("cannot execute rpcName=%q on vmstorage %q with timeout %s: %w", rpcName, remoteAddr, deadline.String(), err)
	}
	// Return the connection back to the pool, assuming it is healthy.
	sn.connPool.Put(bc)
	return nil
}

type errRemote struct {
	msg string
}

func (er *errRemote) Error() string {
	return er.msg
}

func newErrRemote(buf []byte) error {
	err := &errRemote{
		msg: string(buf),
	}
	if !strings.Contains(err.msg, "denyQueriesOutsideRetention") {
		return err
	}
	return &httpserver.ErrorWithStatusCode{
		Err:        err,
		StatusCode: http.StatusServiceUnavailable,
	}
}

const maxLabelSize = 16 * 1024 * 1024

func (sn *storageNode) getLabelsOnConn(bc *handshake.BufferedConn) ([]string, error) {
	// Send the request to sn.
	if err := bc.Flush(); err != nil {
		return nil, fmt.Errorf("cannot flush request to conn: %w", err)
	}

	// Read response error.
	buf, err := readBytes(nil, bc, maxErrorMessageSize)
	if err != nil {
		return nil, fmt.Errorf("cannot read error message: %w", err)
	}
	if len(buf) > 0 {
		return nil, newErrRemote(buf)
	}

	// Read response
	var labels []string
	for {
		buf, err = readBytes(buf[:0], bc, maxLabelSize)
		if err != nil {
			return nil, fmt.Errorf("cannot read labels: %w", err)
		}
		if len(buf) == 0 {
			// Reached the end of the response
			return labels, nil
		}
		labels = append(labels, string(buf))
	}
}

const maxLabelValueSize = 16 * 1024 * 1024

func (sn *storageNode) getLabelValuesOnTimeRangeOnConn(bc *handshake.BufferedConn, labelName string, sr storage.ScanRange) ([]string, error) {
	// Send the request to sn.
	if err := writeBytes(bc, []byte(labelName)); err != nil {
		return nil, fmt.Errorf("cannot send labelName=%q to conn: %w", labelName, err)
	}
	if err := writeTimeRange(bc, sr); err != nil {
		return nil, err
	}
	if err := bc.Flush(); err != nil {
		return nil, fmt.Errorf("cannot flush labelName to conn: %w", err)
	}

	// Read response error.
	buf, err := readBytes(nil, bc, maxErrorMessageSize)
	if err != nil {
		return nil, fmt.Errorf("cannot read error message: %w", err)
	}
	if len(buf) > 0 {
		return nil, newErrRemote(buf)
	}

	// Read response
	labelValues, _, err := readLabelValues(buf, bc)
	if err != nil {
		return nil, err
	}
	return labelValues, nil
}

func (sn *storageNode) getLabelValuesOnConn(bc *handshake.BufferedConn, labelName string) ([]string, error) {
	// Send the request to sn.
	if err := writeBytes(bc, []byte(labelName)); err != nil {
		return nil, fmt.Errorf("cannot send labelName=%q to conn: %w", labelName, err)
	}
	if err := bc.Flush(); err != nil {
		return nil, fmt.Errorf("cannot flush labelName to conn: %w", err)
	}

	// Read response error.
	buf, err := readBytes(nil, bc, maxErrorMessageSize)
	if err != nil {
		return nil, fmt.Errorf("cannot read error message: %w", err)
	}
	if len(buf) > 0 {
		return nil, newErrRemote(buf)
	}

	// Read response
	labelValues, _, err := readLabelValues(buf, bc)
	if err != nil {
		return nil, err
	}
	return labelValues, nil
}

func readLabelValues(buf []byte, bc *handshake.BufferedConn) ([]string, []byte, error) {
	var labelValues []string
	for {
		var err error
		buf, err = readBytes(buf[:0], bc, maxLabelValueSize)
		if err != nil {
			return nil, buf, fmt.Errorf("cannot read labelValue: %w", err)
		}
		if len(buf) == 0 {
			// Reached the end of the response
			return labelValues, buf, nil
		}
		labelValues = append(labelValues, string(buf))
	}
}

func (sn *storageNode) getTagValueSuffixesOnConn(bc *handshake.BufferedConn, tr storage.ScanRange, tagKey, tagValuePrefix string, delimiter byte) ([]string, error) {
	// Send the request to sn.
	if err := writeTimeRange(bc, tr); err != nil {
		return nil, err
	}
	if err := writeBytes(bc, []byte(tagKey)); err != nil {
		return nil, fmt.Errorf("cannot send tagKey=%q to conn: %w", tagKey, err)
	}
	if err := writeBytes(bc, []byte(tagValuePrefix)); err != nil {
		return nil, fmt.Errorf("cannot send tagValuePrefix=%q to conn: %w", tagValuePrefix, err)
	}
	if err := writeByte(bc, delimiter); err != nil {
		return nil, fmt.Errorf("cannot send delimiter=%c to conn: %w", delimiter, err)
	}
	if err := bc.Flush(); err != nil {
		return nil, fmt.Errorf("cannot flush request to conn: %w", err)
	}

	// Read response error.
	buf, err := readBytes(nil, bc, maxErrorMessageSize)
	if err != nil {
		return nil, fmt.Errorf("cannot read error message: %w", err)
	}
	if len(buf) > 0 {
		return nil, newErrRemote(buf)
	}

	// Read response.
	// The response may contain empty suffix, so it is prepended with the number of the following suffixes.
	suffixesCount, err := readUint64(bc)
	if err != nil {
		return nil, fmt.Errorf("cannot read the number of tag value suffixes: %w", err)
	}
	suffixes := make([]string, 0, suffixesCount)
	for i := 0; i < int(suffixesCount); i++ {
		buf, err = readBytes(buf[:0], bc, maxLabelValueSize)
		if err != nil {
			return nil, fmt.Errorf("cannot read tag value suffix #%d: %w", i+1, err)
		}
		suffixes = append(suffixes, string(buf))
	}
	return suffixes, nil
}

func (sn *storageNode) getLabelEntriesOnConn(bc *handshake.BufferedConn) ([]storage.TagEntry, error) {
	// Send the request to sn.
	if err := bc.Flush(); err != nil {
		return nil, fmt.Errorf("cannot flush request to conn: %w", err)
	}

	// Read response error.
	buf, err := readBytes(nil, bc, maxErrorMessageSize)
	if err != nil {
		return nil, fmt.Errorf("cannot read error message: %w", err)
	}
	if len(buf) > 0 {
		return nil, newErrRemote(buf)
	}

	// Read response.
	var labelEntries []storage.TagEntry
	for {
		buf, err = readBytes(buf[:0], bc, maxLabelSize)
		if err != nil {
			return nil, fmt.Errorf("cannot read label: %w", err)
		}
		if len(buf) == 0 {
			// Reached the end of the response
			return labelEntries, nil
		}
		label := string(buf)
		var values []string
		values, buf, err = readLabelValues(buf, bc)
		if err != nil {
			return nil, fmt.Errorf("cannot read values for label %q: %w", label, err)
		}
		labelEntries = append(labelEntries, storage.TagEntry{
			Key:    label,
			Values: values,
		})
	}
}

func (sn *storageNode) getSeriesCountOnConn(bc *handshake.BufferedConn) (uint64, error) {
	// Send the request to sn.
	if err := bc.Flush(); err != nil {
		return 0, fmt.Errorf("cannot flush seriesCount args to conn: %w", err)
	}

	// Read response error.
	buf, err := readBytes(nil, bc, maxErrorMessageSize)
	if err != nil {
		return 0, fmt.Errorf("cannot read error message: %w", err)
	}
	if len(buf) > 0 {
		return 0, newErrRemote(buf)
	}

	// Read response
	n, err := readUint64(bc)
	if err != nil {
		return 0, fmt.Errorf("cannot read series count: %w", err)
	}
	return n, nil
}

// maxMetricBlockSize is the maximum size of serialized MetricBlock.
const maxMetricBlockSize = 1024 * 1024

// maxErrorMessageSize is the maximum size of error message received
// from vmstorage.
const maxErrorMessageSize = 64 * 1024

func (sn *storageNode) processSearchTraceIDsOnConn(bc *handshake.BufferedConn, requestData []byte) ([]storage.TraceID, error) {
	// Send the requst to sn.
	if err := writeBytes(bc, requestData); err != nil {
		return nil, fmt.Errorf("cannot write requestData: %w", err)
	}
	if err := bc.Flush(); err != nil {
		return nil, fmt.Errorf("cannot flush requestData to conn: %w", err)
	}

	// Read response error.
	buf, err := readBytes(nil, bc, maxErrorMessageSize)
	if err != nil {
		return nil, fmt.Errorf("cannot read error message: %w", err)
	}
	if len(buf) > 0 {
		return nil, newErrRemote(buf)
	}

	// Read metricNames from response.
	traceIDsCount, err := readUint64(bc)
	if err != nil {
		return nil, fmt.Errorf("cannot read metricNamesCount: %w", err)
	}
	traceIDs := make([]storage.TraceID, traceIDsCount)
	for i := int64(0); i < int64(traceIDsCount); i++ {
		buf, err = readBytes(buf[:0], bc, maxTraceIDSize)
		if err != nil {
			return nil, fmt.Errorf("cannot read traceID #%d: %w", i+1, err)
		}
		traceID, _, err := storage.UnmarshalTraceID(buf)
		if err != nil {
			return nil, fmt.Errorf("cannot read traceID #%d: %w", i+1, err)
		}

		traceIDs = append(traceIDs, traceID)
	}
	return traceIDs, nil
}

const maxTraceIDSize = 256

func (sn *storageNode) processSearchQueryOnConn(bc *handshake.BufferedConn, requestData []byte, processBlock func(mb *storage.Block) error) (int, error) {
	// Send the request to sn.
	if err := writeBytes(bc, requestData); err != nil {
		return 0, fmt.Errorf("cannot write requestData: %w", err)
	}
	if err := bc.Flush(); err != nil {
		return 0, fmt.Errorf("cannot flush requestData to conn: %w", err)
	}

	// Read response error.
	buf, err := readBytes(nil, bc, maxErrorMessageSize)
	if err != nil {
		return 0, fmt.Errorf("cannot read error message: %w", err)
	}
	if len(buf) > 0 {
		return 0, newErrRemote(buf)
	}

	// Read response. It may consist of multiple MetricBlocks.
	blocksRead := 0
	var mb storage.Block
	for {
		buf, err = readBytes(buf[:0], bc, maxMetricBlockSize)
		if err != nil {
			return blocksRead, fmt.Errorf("cannot read MetricBlock #%d: %w", blocksRead, err)
		}
		if len(buf) == 0 {
			// Reached the end of the response
			return blocksRead, nil
		}
		tail, err := storage.UnmarshalBlock(&mb, buf)
		if err != nil {
			return blocksRead, fmt.Errorf("cannot unmarshal MetricBlock #%d: %w", blocksRead, err)
		}
		if len(tail) != 0 {
			return blocksRead, fmt.Errorf("non-empty tail after unmarshaling MetricBlock #%d: (len=%d) %q", blocksRead, len(tail), tail)
		}
		blocksRead++
		sn.metricBlocksRead.Inc()
		sn.metricRowsRead.Add(mb.RowsCount())
		if err := processBlock(&mb); err != nil {
			return blocksRead, fmt.Errorf("cannot process MetricBlock #%d: %w", blocksRead, err)
		}
	}
}

func writeTimeRange(bc *handshake.BufferedConn, tr storage.ScanRange) error {
	if err := writeUint64(bc, uint64(tr.MinTimestamp)); err != nil {
		return fmt.Errorf("cannot send minTimestamp=%d to conn: %w", tr.MinTimestamp, err)
	}
	if err := writeUint64(bc, uint64(tr.MaxTimestamp)); err != nil {
		return fmt.Errorf("cannot send maxTimestamp=%d to conn: %w", tr.MaxTimestamp, err)
	}
	return nil
}

func writeBytes(bc *handshake.BufferedConn, buf []byte) error {
	sizeBuf := encoding.MarshalUint64(nil, uint64(len(buf)))
	if _, err := bc.Write(sizeBuf); err != nil {
		return err
	}
	_, err := bc.Write(buf)
	return err
}

func writeUint32(bc *handshake.BufferedConn, n uint32) error {
	buf := encoding.MarshalUint32(nil, n)
	_, err := bc.Write(buf)
	return err
}

func writeUint64(bc *handshake.BufferedConn, n uint64) error {
	buf := encoding.MarshalUint64(nil, n)
	_, err := bc.Write(buf)
	return err
}

func writeBool(bc *handshake.BufferedConn, b bool) error {
	var buf [1]byte
	if b {
		buf[0] = 1
	}
	_, err := bc.Write(buf[:])
	return err
}

func writeByte(bc *handshake.BufferedConn, b byte) error {
	var buf [1]byte
	buf[0] = b
	_, err := bc.Write(buf[:])
	return err
}

func readBytes(buf []byte, bc *handshake.BufferedConn, maxDataSize int) ([]byte, error) {
	buf = bytesutil.Resize(buf, 8)
	if n, err := io.ReadFull(bc, buf); err != nil {
		return buf, fmt.Errorf("cannot read %d bytes with data size: %w; read only %d bytes", len(buf), err, n)
	}
	dataSize := encoding.UnmarshalUint64(buf)
	if dataSize > uint64(maxDataSize) {
		return buf, fmt.Errorf("too big data size: %d; it mustn't exceed %d bytes", dataSize, maxDataSize)
	}
	buf = bytesutil.Resize(buf, int(dataSize))
	if dataSize == 0 {
		return buf, nil
	}
	if n, err := io.ReadFull(bc, buf); err != nil {
		return buf, fmt.Errorf("cannot read data with size %d: %w; read only %d bytes", dataSize, err, n)
	}
	return buf, nil
}

func readUint64(bc *handshake.BufferedConn) (uint64, error) {
	var buf [8]byte
	if _, err := io.ReadFull(bc, buf[:]); err != nil {
		return 0, fmt.Errorf("cannot read uint64: %w", err)
	}
	n := encoding.UnmarshalUint64(buf[:])
	return n, nil
}

var storageNodes []*storageNode

// InitStorageNodes initializes storage nodes' connections to the given addrs.
func InitStorageNodes(addrs []string) {
	if len(addrs) == 0 {
		logger.Panicf("BUG: addrs must be non-empty")
	}

	for _, addr := range addrs {
		sn := &storageNode{
			// There is no need in requests compression, since they are usually very small.
			connPool: netutil.NewConnPool("vmselect", addr, handshake.VMSelectClient, 0),

			concurrentQueriesCh: make(chan struct{}, maxConcurrentQueriesPerStorageNode),

			registerMetricNamesRequests:    metrics.NewCounter(fmt.Sprintf(`vm_requests_total{action="registerMetricNames", type="rpcClient", name="vmselect", addr=%q}`, addr)),
			registerMetricNamesErrors:      metrics.NewCounter(fmt.Sprintf(`vm_request_errors_total{action="registerMetricNames", type="rpcClient", name="vmselect", addr=%q}`, addr)),
			deleteSeriesRequests:           metrics.NewCounter(fmt.Sprintf(`vm_requests_total{action="deleteSeries", type="rpcClient", name="vmselect", addr=%q}`, addr)),
			deleteSeriesErrors:             metrics.NewCounter(fmt.Sprintf(`vm_request_errors_total{action="deleteSeries", type="rpcClient", name="vmselect", addr=%q}`, addr)),
			labelsOnTimeRangeRequests:      metrics.NewCounter(fmt.Sprintf(`vm_requests_total{action="labelsOnTimeRange", type="rpcClient", name="vmselect", addr=%q}`, addr)),
			labelsRequests:                 metrics.NewCounter(fmt.Sprintf(`vm_requests_total{action="labels", type="rpcClient", name="vmselect", addr=%q}`, addr)),
			labelsOnTimeRangeErrors:        metrics.NewCounter(fmt.Sprintf(`vm_request_errors_total{action="labelsOnTimeRange", type="rpcClient", name="vmselect", addr=%q}`, addr)),
			labelsErrors:                   metrics.NewCounter(fmt.Sprintf(`vm_request_errors_total{action="labels", type="rpcClient", name="vmselect", addr=%q}`, addr)),
			labelValuesOnTimeRangeRequests: metrics.NewCounter(fmt.Sprintf(`vm_requests_total{action="labelValuesOnTimeRange", type="rpcClient", name="vmselect", addr=%q}`, addr)),
			labelValuesRequests:            metrics.NewCounter(fmt.Sprintf(`vm_requests_total{action="labelValues", type="rpcClient", name="vmselect", addr=%q}`, addr)),
			labelValuesOnTimeRangeErrors:   metrics.NewCounter(fmt.Sprintf(`vm_request_errors_total{action="labelValuesOnTimeRange", type="rpcClient", name="vmselect", addr=%q}`, addr)),
			labelValuesErrors:              metrics.NewCounter(fmt.Sprintf(`vm_request_errors_total{action="labelValues", type="rpcClient", name="vmselect", addr=%q}`, addr)),
			labelEntriesRequests:           metrics.NewCounter(fmt.Sprintf(`vm_requests_total{action="labelEntries", type="rpcClient", name="vmselect", addr=%q}`, addr)),
			labelEntriesErrors:             metrics.NewCounter(fmt.Sprintf(`vm_request_errors_total{action="labelEntries", type="rpcClient", name="vmselect", addr=%q}`, addr)),
			tagValueSuffixesRequests:       metrics.NewCounter(fmt.Sprintf(`vm_requests_total{action="tagValueSuffixes", type="rpcClient", name="vmselect", addr=%q}`, addr)),
			tagValueSuffixesErrors:         metrics.NewCounter(fmt.Sprintf(`vm_request_errors_total{action="tagValueSuffixes", type="rpcClient", name="vmselect", addr=%q}`, addr)),
			tsdbStatusRequests:             metrics.NewCounter(fmt.Sprintf(`vm_requests_total{action="tsdbStatus", type="rpcClient", name="vmselect", addr=%q}`, addr)),
			tsdbStatusErrors:               metrics.NewCounter(fmt.Sprintf(`vm_request_errors_total{action="tsdbStatus", type="rpcClient", name="vmselect", addr=%q}`, addr)),
			seriesCountRequests:            metrics.NewCounter(fmt.Sprintf(`vm_requests_total{action="seriesCount", type="rpcClient", name="vmselect", addr=%q}`, addr)),
			seriesCountErrors:              metrics.NewCounter(fmt.Sprintf(`vm_request_errors_total{action="seriesCount", type="rpcClient", name="vmselect", addr=%q}`, addr)),
			searchMetricNamesRequests:      metrics.NewCounter(fmt.Sprintf(`vm_requests_total{action="searchMetricNames", type="rpcClient", name="vmselect", addr=%q}`, addr)),
			searchRequests:                 metrics.NewCounter(fmt.Sprintf(`vm_requests_total{action="search", type="rpcClient", name="vmselect", addr=%q}`, addr)),
			searchMetricNamesErrors:        metrics.NewCounter(fmt.Sprintf(`vm_request_errors_total{action="searchMetricNames", type="rpcClient", name="vmselect", addr=%q}`, addr)),
			searchErrors:                   metrics.NewCounter(fmt.Sprintf(`vm_request_errors_total{action="search", type="rpcClient", name="vmselect", addr=%q}`, addr)),
			metricBlocksRead:               metrics.NewCounter(fmt.Sprintf(`vm_metric_blocks_read_total{name="vmselect", addr=%q}`, addr)),
			metricRowsRead:                 metrics.NewCounter(fmt.Sprintf(`vm_metric_rows_read_total{name="vmselect", addr=%q}`, addr)),
		}
		metrics.NewGauge(fmt.Sprintf(`vm_concurrent_queries{name="vmselect", addr=%q}`, addr), func() float64 {
			return float64(len(sn.concurrentQueriesCh))
		})
		storageNodes = append(storageNodes, sn)
	}
}

// Stop gracefully stops netstorage.
func Stop() {
	// Nothing to do at the moment.
}

var (
	partialLabelsOnTimeRangeResults      = metrics.NewCounter(`vm_partial_results_total{type="labels_on_time_range", name="vmselect"}`)
	partialLabelsResults                 = metrics.NewCounter(`vm_partial_results_total{type="labels", name="vmselect"}`)
	partialLabelValuesOnTimeRangeResults = metrics.NewCounter(`vm_partial_results_total{type="label_values_on_time_range", name="vmselect"}`)
	partialLabelValuesResults            = metrics.NewCounter(`vm_partial_results_total{type="label_values", name="vmselect"}`)
	partialTagValueSuffixesResults       = metrics.NewCounter(`vm_partial_results_total{type="tag_value_suffixes", name="vmselect"}`)
	partialLabelEntriesResults           = metrics.NewCounter(`vm_partial_results_total{type="label_entries", name="vmselect"}`)
	partialTSDBStatusResults             = metrics.NewCounter(`vm_partial_results_total{type="tsdb_status", name="vmselect"}`)
	partialSeriesCountResults            = metrics.NewCounter(`vm_partial_results_total{type="series_count", name="vmselect"}`)
	partialSearchMetricNamesResults      = metrics.NewCounter(`vm_partial_results_total{type="search_metric_names", name="vmselect"}`)
	partialSearchResults                 = metrics.NewCounter(`vm_partial_results_total{type="search", name="vmselect"}`)
)

// The maximum number of concurrent queries per storageNode.
const maxConcurrentQueriesPerStorageNode = 100
