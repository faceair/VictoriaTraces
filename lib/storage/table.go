package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// table represents a single table with time series data.
type table struct {
	path                string
	smallPartitionsPath string
	bigPartitionsPath   string

	retentionMsecs int64

	ptws     []*partitionWrapper
	ptwsLock sync.Mutex

	flockF *os.File

	stop chan struct{}

	retentionWatcherWG sync.WaitGroup
}

// partitionWrapper provides refcounting mechanism for the partition.
type partitionWrapper struct {
	// Atomic counters must be at the top of struct for proper 8-byte alignment on 32-bit archs.
	// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/212

	refCount uint64

	// The partition must be dropped if mustDrop > 0
	mustDrop uint64

	pt *partition
}

func (ptw *partitionWrapper) incRef() {
	atomic.AddUint64(&ptw.refCount, 1)
}

func (ptw *partitionWrapper) decRef() {
	n := atomic.AddUint64(&ptw.refCount, ^uint64(0))
	if int64(n) < 0 {
		logger.Panicf("BUG: pts.refCount must be positive; got %d", int64(n))
	}
	if n > 0 {
		return
	}

	// refCount is zero. Close the partition.
	ptw.pt.MustClose()

	if atomic.LoadUint64(&ptw.mustDrop) == 0 {
		ptw.pt = nil
		return
	}

	// ptw.mustDrop > 0. Drop the partition.
	ptw.pt.Drop()
	ptw.pt = nil
}

func (ptw *partitionWrapper) scheduleToDrop() {
	atomic.AddUint64(&ptw.mustDrop, 1)
}

// openTable opens a table on the given path with the given retentionMsecs.
//
// The table is created if it doesn't exist.
//
// Data older than the retentionMsecs may be dropped at any time.
func openTable(path string, retentionMsecs int64) (*table, error) {
	path = filepath.Clean(path)

	// Create a directory for the table if it doesn't exist yet.
	if err := fs.MkdirAllIfNotExist(path); err != nil {
		return nil, fmt.Errorf("cannot create directory for table %q: %w", path, err)
	}

	// Protect from concurrent opens.
	flockF, err := fs.CreateFlockFile(path)
	if err != nil {
		return nil, err
	}

	// Create directories for small and big partitions if they don't exist yet.
	smallPartitionsPath := path + "/small"
	if err := fs.MkdirAllIfNotExist(smallPartitionsPath); err != nil {
		return nil, fmt.Errorf("cannot create directory for small partitions %q: %w", smallPartitionsPath, err)
	}
	bigPartitionsPath := path + "/big"
	if err := fs.MkdirAllIfNotExist(bigPartitionsPath); err != nil {
		return nil, fmt.Errorf("cannot create directory for big partitions %q: %w", bigPartitionsPath, err)
	}

	// Open partitions.
	pts, err := openPartitions(smallPartitionsPath, bigPartitionsPath, retentionMsecs)
	if err != nil {
		return nil, fmt.Errorf("cannot open partitions in the table %q: %w", path, err)
	}

	tb := &table{
		path:                path,
		smallPartitionsPath: smallPartitionsPath,
		bigPartitionsPath:   bigPartitionsPath,
		retentionMsecs:      retentionMsecs,

		flockF: flockF,

		stop: make(chan struct{}),
	}
	for _, pt := range pts {
		tb.addPartitionNolock(pt)
	}
	tb.startRetentionWatcher()
	return tb, nil
}

func (tb *table) addPartitionNolock(pt *partition) {
	ptw := &partitionWrapper{
		pt:       pt,
		refCount: 1,
	}
	tb.ptws = append(tb.ptws, ptw)
}

// MustClose closes the table.
func (tb *table) MustClose() {
	close(tb.stop)
	tb.retentionWatcherWG.Wait()

	tb.ptwsLock.Lock()
	ptws := tb.ptws
	tb.ptws = nil
	tb.ptwsLock.Unlock()

	// Decrement references to partitions, so they may be eventually closed after
	// pending searches are done.
	for _, ptw := range ptws {
		ptw.decRef()
	}

	// Release exclusive lock on the table.
	if err := tb.flockF.Close(); err != nil {
		logger.Panicf("FATAL: cannot release lock on %q: %s", tb.flockF.Name(), err)
	}
}

// flushRawRows flushes all the pending rows, so they become visible to search.
//
// This function is for debug purposes only.
func (tb *table) flushRawRows() {
	ptws := tb.GetPartitions(nil)
	defer tb.PutPartitions(ptws)

	for _, ptw := range ptws {
		ptw.pt.flushRawRows(true)
	}
}

// TableMetrics contains essential metrics for the table.
type TableMetrics struct {
	partitionMetrics

	PartitionsRefCount uint64
}

// UpdateMetrics updates m with metrics from tb.
func (tb *table) UpdateMetrics(m *TableMetrics) {
	tb.ptwsLock.Lock()
	for _, ptw := range tb.ptws {
		ptw.pt.UpdateMetrics(&m.partitionMetrics)
		m.PartitionsRefCount += atomic.LoadUint64(&ptw.refCount)
	}
	tb.ptwsLock.Unlock()
}

// ForceMergePartitions force-merges partitions in tb with names starting from the given partitionNamePrefix.
//
// Partitions are merged sequentially in order to reduce load on the system.
func (tb *table) ForceMergePartitions(partitionNamePrefix string) error {
	ptws := tb.GetPartitions(nil)
	defer tb.PutPartitions(ptws)
	for _, ptw := range ptws {
		if !strings.HasPrefix(ptw.pt.name, partitionNamePrefix) {
			continue
		}
		logger.Infof("starting forced merge for partition %q", ptw.pt.name)
		startTime := time.Now()
		if err := ptw.pt.ForceMergeAllParts(); err != nil {
			return fmt.Errorf("cannot complete forced merge for partition %q: %w", ptw.pt.name, err)
		}
		logger.Infof("forced merge for partition %q has been finished in %.3f seconds", ptw.pt.name, time.Since(startTime).Seconds())
	}
	return nil
}

// AddRows adds the given rows to the table tb.
func (tb *table) AddRows(rows []rawRow) error {
	if len(rows) == 0 {
		return nil
	}

	// Verify whether all the rows may be added to a single partition.
	ptwsX := getPartitionWrappers()
	defer putPartitionWrappers(ptwsX)

	ptwsX.a = tb.GetPartitions(ptwsX.a[:0])
	ptws := ptwsX.a
	for _, ptw := range ptws {
		singlePt := true
		for i := range rows {
			if !ptw.pt.HasTimestamp(rows[i].Timestamp) {
				singlePt = false
				break
			}
		}
		if !singlePt {
			continue
		}

		// Move the partition with the matching rows to the front of tb.ptws,
		// so it will be detected faster next time.
		tb.ptwsLock.Lock()
		for i := range tb.ptws {
			if ptw == tb.ptws[i] {
				tb.ptws[0], tb.ptws[i] = tb.ptws[i], tb.ptws[0]
				break
			}
		}
		tb.ptwsLock.Unlock()

		// Fast path - add all the rows into the ptw.
		ptw.pt.AddRows(rows)
		tb.PutPartitions(ptws)
		return nil
	}

	// Slower path - split rows into per-partition buckets.
	ptBuckets := make(map[*partitionWrapper][]rawRow)
	var missingRows []rawRow
	for i := range rows {
		r := &rows[i]
		ptFound := false
		for _, ptw := range ptws {
			if ptw.pt.HasTimestamp(r.Timestamp) {
				ptBuckets[ptw] = append(ptBuckets[ptw], *r)
				ptFound = true
				break
			}
		}
		if !ptFound {
			missingRows = append(missingRows, *r)
		}
	}

	for ptw, ptRows := range ptBuckets {
		ptw.pt.AddRows(ptRows)
	}
	tb.PutPartitions(ptws)
	if len(missingRows) == 0 {
		return nil
	}

	// The slowest path - there are rows that don't fit any existing partition.
	// Create new partitions for these rows.
	// Do this under tb.ptwsLock.
	minTimestamp, maxTimestamp := tb.getMinMaxTimestamps()
	tb.ptwsLock.Lock()
	var errors []error
	for i := range missingRows {
		r := &missingRows[i]

		if r.Timestamp < minTimestamp || r.Timestamp > maxTimestamp {
			// Silently skip row outside retention, since it should be deleted anyway.
			continue
		}

		// Make sure the partition for the r hasn't been added by another goroutines.
		ptFound := false
		for _, ptw := range tb.ptws {
			if ptw.pt.HasTimestamp(r.Timestamp) {
				ptFound = true
				ptw.pt.AddRows(missingRows[i : i+1])
				break
			}
		}
		if ptFound {
			continue
		}

		pt, err := createPartition(r.Timestamp, tb.smallPartitionsPath, tb.bigPartitionsPath, tb.retentionMsecs)
		if err != nil {
			errors = append(errors, err)
			continue
		}
		pt.AddRows(missingRows[i : i+1])
		tb.addPartitionNolock(pt)
	}
	tb.ptwsLock.Unlock()

	if len(errors) > 0 {
		// Return only the first error, since it has no sense in returning all errors.
		return fmt.Errorf("errors while adding rows to table %q: %w", tb.path, errors[0])
	}
	return nil
}

func (tb *table) getMinMaxTimestamps() (int64, int64) {
	now := int64(fasttime.UnixTimestamp() * 1000)
	minTimestamp := now - tb.retentionMsecs
	maxTimestamp := now + 2*24*3600*1000 // allow max +2 days from now due to timezones shit :)
	if minTimestamp < 0 {
		// Negative timestamps aren't supported by the storage.
		minTimestamp = 0
	}
	if maxTimestamp < 0 {
		maxTimestamp = (1 << 63) - 1
	}
	return minTimestamp, maxTimestamp
}

func (tb *table) startRetentionWatcher() {
	tb.retentionWatcherWG.Add(1)
	go func() {
		tb.retentionWatcher()
		tb.retentionWatcherWG.Done()
	}()
}

func (tb *table) retentionWatcher() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-tb.stop:
			return
		case <-ticker.C:
		}

		minTimestamp := int64(fasttime.UnixTimestamp()*1000) - tb.retentionMsecs
		var ptwsDrop []*partitionWrapper
		tb.ptwsLock.Lock()
		dst := tb.ptws[:0]
		for _, ptw := range tb.ptws {
			if ptw.pt.tr.MaxTimestamp < minTimestamp {
				ptwsDrop = append(ptwsDrop, ptw)
			} else {
				dst = append(dst, ptw)
			}
		}
		tb.ptws = dst
		tb.ptwsLock.Unlock()

		if len(ptwsDrop) == 0 {
			continue
		}

		// There are paritions to drop. Drop them.

		// Remove table references from partitions, so they will be eventually
		// closed and dropped after all the pending searches are done.
		for _, ptw := range ptwsDrop {
			ptw.scheduleToDrop()
			ptw.decRef()
		}
	}
}

// GetPartitions appends tb's partitions snapshot to dst and returns the result.
//
// The returned partitions must be passed to PutPartitions
// when they no longer needed.
func (tb *table) GetPartitions(dst []*partitionWrapper) []*partitionWrapper {
	tb.ptwsLock.Lock()
	for _, ptw := range tb.ptws {
		ptw.incRef()
		dst = append(dst, ptw)
	}
	tb.ptwsLock.Unlock()

	return dst
}

// PutPartitions deregisters ptws obtained via GetPartitions.
func (tb *table) PutPartitions(ptws []*partitionWrapper) {
	for _, ptw := range ptws {
		ptw.decRef()
	}
}

func openPartitions(smallPartitionsPath, bigPartitionsPath string, retentionMsecs int64) ([]*partition, error) {
	// Certain partition directories in either `big` or `small` dir may be missing
	// after restoring from backup. So populate partition names from both dirs.
	ptNames := make(map[string]bool)
	if err := populatePartitionNames(smallPartitionsPath, ptNames); err != nil {
		return nil, err
	}
	if err := populatePartitionNames(bigPartitionsPath, ptNames); err != nil {
		return nil, err
	}
	var pts []*partition
	for ptName := range ptNames {
		smallPartsPath := smallPartitionsPath + "/" + ptName
		bigPartsPath := bigPartitionsPath + "/" + ptName
		pt, err := openPartition(smallPartsPath, bigPartsPath, retentionMsecs)
		if err != nil {
			mustClosePartitions(pts)
			return nil, fmt.Errorf("cannot open partition %q: %w", ptName, err)
		}
		pts = append(pts, pt)
	}
	return pts, nil
}

func populatePartitionNames(partitionsPath string, ptNames map[string]bool) error {
	d, err := os.Open(partitionsPath)
	if err != nil {
		return fmt.Errorf("cannot open directory with partitions %q: %w", partitionsPath, err)
	}
	defer fs.MustClose(d)

	fis, err := d.Readdir(-1)
	if err != nil {
		return fmt.Errorf("cannot read directory with partitions %q: %w", partitionsPath, err)
	}
	for _, fi := range fis {
		if !fs.IsDirOrSymlink(fi) {
			// Skip non-directories
			continue
		}
		ptName := fi.Name()
		ptNames[ptName] = true
	}
	return nil
}

func mustClosePartitions(pts []*partition) {
	for _, pt := range pts {
		pt.MustClose()
	}
}

type partitionWrappers struct {
	a []*partitionWrapper
}

func getPartitionWrappers() *partitionWrappers {
	v := ptwsPool.Get()
	if v == nil {
		return &partitionWrappers{}
	}
	return v.(*partitionWrappers)
}

func putPartitionWrappers(ptwsX *partitionWrappers) {
	ptwsX.a = ptwsX.a[:0]
	ptwsPool.Put(ptwsX)
}

var ptwsPool sync.Pool
