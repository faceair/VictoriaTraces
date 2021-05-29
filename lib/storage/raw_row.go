package storage

import (
	"sort"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// rawRow reperesents raw timeseries row.
type rawRow struct {
	// TraceID is time series id.
	TraceID TraceID

	MetricID uint64

	// Timestamp is unix timestamp in milliseconds.
	Timestamp int64

	// Value is time series value for the given timestamp.
	Value []byte
}

type rawRowsMarshaler struct {
	bsw blockStreamWriter

	auxTimestamps  []int64
	auxValues      [][]byte
	auxFloatValues [][]byte
}

func (rrm *rawRowsMarshaler) reset() {
	rrm.bsw.reset()

	rrm.auxTimestamps = rrm.auxTimestamps[:0]
	rrm.auxValues = rrm.auxValues[:0]
	rrm.auxFloatValues = rrm.auxFloatValues[:0]
}

// Use sort.Interface instead of sort.Slice in order to optimize rows swap.
type rawRowsSort []rawRow

func (rrs *rawRowsSort) Len() int { return len(*rrs) }
func (rrs *rawRowsSort) Less(i, j int) bool {
	x := *rrs
	if i < 0 || j < 0 || i >= len(x) || j >= len(x) {
		// This is no-op for compiler, so it doesn't generate panic code
		// for out of range access on x[i], x[j] below
		return false
	}
	a := &x[i]
	b := &x[j]

	if !a.TraceID.Equals(&b.TraceID) {
		return a.TraceID.Less(&b.TraceID)
	}
	return a.Timestamp < b.Timestamp
}
func (rrs *rawRowsSort) Swap(i, j int) {
	x := *rrs
	x[i], x[j] = x[j], x[i]
}

func (rrm *rawRowsMarshaler) marshalToInmemoryPart(mp *inmemoryPart, rows []rawRow) {
	if len(rows) == 0 {
		return
	}
	if uint64(len(rows)) >= 1<<32 {
		logger.Panicf("BUG: rows count must be smaller than 2^32; got %d", len(rows))
	}

	rrm.bsw.InitFromInmemoryPart(mp)

	ph := &mp.ph
	ph.Reset()

	// Sort rows by (TraceID, Timestamp) if they aren't sorted yet.
	rrs := rawRowsSort(rows)
	if !sort.IsSorted(&rrs) {
		sort.Sort(&rrs)
	}

	// Group rows into blocks.
	var rowsMerged uint64
	r := &rows[0]
	traceID := r.TraceID
	tmpBlock := getBlock()
	defer putBlock(tmpBlock)
	for i := range rows {
		r = &rows[i]
		if r.TraceID.Equals(&traceID) && len(rrm.auxTimestamps) < maxRowsPerBlock {
			rrm.auxTimestamps = append(rrm.auxTimestamps, r.Timestamp)
			rrm.auxFloatValues = append(rrm.auxFloatValues, r.Value)
			continue
		}

		rrm.auxValues = append(rrm.auxValues[:0], rrm.auxFloatValues...)
		tmpBlock.Init(traceID, rrm.auxTimestamps, rrm.auxValues)
		rrm.bsw.WriteExternalBlock(tmpBlock, ph, &rowsMerged)

		traceID = r.TraceID
		rrm.auxTimestamps = append(rrm.auxTimestamps[:0], r.Timestamp)
		rrm.auxFloatValues = append(rrm.auxFloatValues[:0], r.Value)
	}

	rrm.auxValues = append(rrm.auxValues[:0], rrm.auxFloatValues...)
	tmpBlock.Init(traceID, rrm.auxTimestamps, rrm.auxValues)
	rrm.bsw.WriteExternalBlock(tmpBlock, ph, &rowsMerged)
	if rowsMerged != uint64(len(rows)) {
		logger.Panicf("BUG: unexpected rowsMerged; got %d; want %d", rowsMerged, len(rows))
	}
	rrm.bsw.MustClose()
}

func getRawRowsMarshaler() *rawRowsMarshaler {
	v := rrmPool.Get()
	if v == nil {
		return &rawRowsMarshaler{}
	}
	return v.(*rawRowsMarshaler)
}

func putRawRowsMarshaler(rrm *rawRowsMarshaler) {
	rrm.reset()
	rrmPool.Put(rrm)
}

var rrmPool sync.Pool
