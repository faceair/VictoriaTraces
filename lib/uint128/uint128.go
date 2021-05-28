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
	Lo uint64
	Hi uint64
}

var marshaledUint128Size = func() int {
	var t Uint128
	dst := t.Marshal(nil)
	return len(dst)
}()

func (t Uint128) Reset() {
	t.Lo, t.Hi = 0, 0
}

func (t Uint128) Marshal(dst []byte) []byte {
	dst = encoding.MarshalUint64(dst, t.Hi)
	dst = encoding.MarshalUint64(dst, t.Lo)
	return dst
}

func (t Uint128) Unmarshal(src []byte) ([]byte, error) {
	if len(src) < marshaledUint128Size {
		return nil, fmt.Errorf("too short src; got %d bytes; want %d bytes", len(src), marshaledUint128Size)
	}

	t.Hi = encoding.UnmarshalUint64(src)
	src = src[8:]
	t.Lo = encoding.UnmarshalUint64(src)
	src = src[8:]
	return src, nil
}

func (t Uint128) Equals(v Uint128) bool {
	return t == v
}

func (t Uint128) Less(v Uint128) bool {
	return t.Hi < v.Hi || (t.Hi == v.Hi && t.Lo < v.Lo)
}

func (t Uint128) String() string {
	if t.Hi == 0 {
		return fmt.Sprintf("%016x", t.Lo)
	}
	return fmt.Sprintf("%016x%016x", t.Hi, t.Lo)
}
