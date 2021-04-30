package vminsert

import (
	"fmt"
	"net/http"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/cespare/xxhash/v2"
	"github.com/faceair/VictoriaTraces/lib/storage"
	"github.com/lithammer/go-jump-consistent-hash"
)

// InsertCtx is a generic context for inserting data.
//
// InsertCtx.Reset must be called before the first usage.
type InsertCtx struct {
	Labels        []storage.Label
	MetricNameBuf []byte

	bufRowss  []bufRows
	labelsBuf []byte
}

type bufRows struct {
	buf  []byte
	rows int
}

func (br *bufRows) reset() {
	br.buf = br.buf[:0]
	br.rows = 0
}

func (br *bufRows) pushTo(sn *storageNode) error {
	bufLen := len(br.buf)
	err := sn.push(br.buf, br.rows)
	br.reset()
	if err != nil {
		return &httpserver.ErrorWithStatusCode{
			Err:        fmt.Errorf("cannot send %d bytes to storageNode %q: %w", bufLen, sn.dialer.Addr(), err),
			StatusCode: http.StatusServiceUnavailable,
		}
	}
	return nil
}

// Reset resets ctx.
func (ctx *InsertCtx) Reset() {
	for i := range ctx.Labels {
		label := &ctx.Labels[i]
		label.Name = nil
		label.Value = nil
	}
	ctx.Labels = ctx.Labels[:0]
	ctx.MetricNameBuf = ctx.MetricNameBuf[:0]

	if ctx.bufRowss == nil {
		ctx.bufRowss = make([]bufRows, len(storageNodes))
	}
	for i := range ctx.bufRowss {
		ctx.bufRowss[i].reset()
	}
	ctx.labelsBuf = ctx.labelsBuf[:0]
}

// AddLabel adds (name, value) label to ctx.Labels.
//
// name and value must exist until ctx.Labels is used.
func (ctx *InsertCtx) AddLabel(name, value []byte) {
	if len(value) == 0 {
		// Skip labels without values, since they have no sense.
		// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/600
		// Do not skip labels with empty name, since they are equal to __name__.
		return
	}
	ctx.Labels = append(ctx.Labels, storage.Label{
		// Do not copy name and value contents for performance reasons.
		// This reduces GC overhead on the number of objects and allocations.
		Name:  name,
		Value: value,
	})
}

// WriteDataPoint writes (timestamp, value) data point with the given at and labels to ctx buffer.
func (ctx *InsertCtx) WriteDataPoint(labels []storage.Label, timestamp int64, value []byte) error {
	ctx.MetricNameBuf = storage.MarshalMetricNameRaw(ctx.MetricNameBuf[:0], labels)
	storageNodeIdx := ctx.GetStorageNodeIdx(labels)
	return ctx.WriteDataPointExt(storageNodeIdx, ctx.MetricNameBuf, timestamp, value)
}

// WriteDataPointExt writes the given metricNameRaw with (timestmap, value) to ctx buffer with the given storageNodeIdx.
func (ctx *InsertCtx) WriteDataPointExt(storageNodeIdx int, metricNameRaw []byte, timestamp int64, value []byte) error {
	br := &ctx.bufRowss[storageNodeIdx]
	sn := storageNodes[storageNodeIdx]
	bufNew := storage.MarshalMetricRow(br.buf, metricNameRaw, timestamp, value)
	if len(bufNew) >= maxBufSizePerStorageNode {
		// Send buf to storageNode, since it is too big.
		if err := br.pushTo(sn); err != nil {
			return err
		}
		br.buf = storage.MarshalMetricRow(bufNew[:0], metricNameRaw, timestamp, value)
	} else {
		br.buf = bufNew
	}
	br.rows++
	return nil
}

// FlushBufs flushes ctx bufs to remote storage nodes.
func (ctx *InsertCtx) FlushBufs() error {
	var firstErr error
	for i := range ctx.bufRowss {
		br := &ctx.bufRowss[i]
		if len(br.buf) == 0 {
			continue
		}
		if err := br.pushTo(storageNodes[i]); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// GetStorageNodeIdx returns storage node index for the given at and labels.
//
// The returned index must be passed to WriteDataPoint.
func (ctx *InsertCtx) GetStorageNodeIdx(labels []storage.Label) int {
	if len(storageNodes) == 1 {
		// Fast path - only a single storage node.
		return 0
	}

	buf := ctx.labelsBuf[:0]
	for i := range labels {
		label := &labels[i]
		buf = marshalBytesFast(buf, label.Name)
		buf = marshalBytesFast(buf, label.Value)
	}
	h := xxhash.Sum64(buf)
	ctx.labelsBuf = buf

	idx := int(jump.Hash(h, int32(len(storageNodes))))
	return idx
}

func marshalBytesFast(dst []byte, s []byte) []byte {
	dst = encoding.MarshalUint16(dst, uint16(len(s)))
	dst = append(dst, s...)
	return dst
}
