package uint128

import (
	"fmt"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
)

func New(lo, hi uint64) Uint128 {
	return Uint128{lo, hi}
}

func Unmarshal(src []byte) (Uint128, []byte, error) {
	u128 := Uint128{}
	src, err := u128.Unmarshal(src)
	return u128, src, err
}

type Uint128 struct {
	lo uint64
	hi uint64
}

var marshaledUint128Size = func() int {
	var t Uint128
	dst := t.Marshal(nil)
	return len(dst)
}()

func (t Uint128) Reset() {
	t.lo, t.hi = 0, 0
}

func (t Uint128) Marshal(dst []byte) []byte {
	dst = encoding.MarshalUint64(dst, t.hi)
	dst = encoding.MarshalUint64(dst, t.lo)
	return dst
}

func (t Uint128) Unmarshal(src []byte) ([]byte, error) {
	if len(src) < marshaledUint128Size {
		return nil, fmt.Errorf("too short src; got %d bytes; want %d bytes", len(src), marshaledUint128Size)
	}

	t.hi = encoding.UnmarshalUint64(src)
	src = src[8:]
	t.lo = encoding.UnmarshalUint64(src)
	src = src[8:]
	return src, nil
}

func (t Uint128) Equals(v Uint128) bool {
	return t == v
}

func (t Uint128) Less(v Uint128) bool {
	return t.hi < v.hi || (t.hi == v.hi && t.lo < v.lo)
}

func (t Uint128) String() string {
	if t.hi == 0 {
		return fmt.Sprintf("%016x", t.lo)
	}
	return fmt.Sprintf("%016x%016x", t.hi, t.lo)
}
