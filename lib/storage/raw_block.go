package storage

import (
	"fmt"
	"strconv"

	"github.com/faceair/VictoriaTraces/lib/uint128"
)

type TraceID = uint128.Uint128

func UnmarshalTraceID(src []byte) (TraceID, []byte, error) {
	return uint128.Unmarshal(src)
}

func parseTraceID(s string) (TraceID, error) {
	var hi, lo uint64
	var err error
	if len(s) > 32 {
		return TraceID{}, fmt.Errorf("traceID cannot be longer than 32 hex characters: %s", s)
	} else if len(s) > 16 {
		hiLen := len(s) - 16
		if hi, err = strconv.ParseUint(s[0:hiLen], 16, 64); err != nil {
			return TraceID{}, err
		}
		if lo, err = strconv.ParseUint(s[hiLen:], 16, 64); err != nil {
			return TraceID{}, err
		}
	} else {
		if lo, err = strconv.ParseUint(s, 16, 64); err != nil {
			return TraceID{}, err
		}
	}
	return uint128.New(lo, hi), nil
}

// rawBlock represents a raw block of a single time-series rows.
type rawBlock struct {
	TraceID    TraceID
	Timestamps []int64
	Values     [][]byte
}

// Reset resets rb.
func (rb *rawBlock) Reset() {
	rb.TraceID = TraceID{}
	rb.Timestamps = rb.Timestamps[:0]
	rb.Values = rb.Values[:0]
}

// CopyFrom copies src to rb.
func (rb *rawBlock) CopyFrom(src *rawBlock) {
	rb.TraceID = src.TraceID
	rb.Timestamps = append(rb.Timestamps[:0], src.Timestamps...)
	rb.Values = append(rb.Values[:0], src.Values...)
}
