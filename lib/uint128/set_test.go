package uint128

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestSetOps(t *testing.T) {
	f := func(a, b []Uint128) {
		t.Helper()
		mUnion := make(map[Uint128]bool)
		mIntersect := make(map[Uint128]bool)
		ma := make(map[Uint128]bool)
		sa := &Set{}
		sb := &Set{}
		for _, v := range a {
			sa.Add(v)
			ma[v] = true
			mUnion[v] = true
		}
		for _, v := range b {
			sb.Add(v)
			mUnion[v] = true
			if ma[v] {
				mIntersect[v] = true
			}
		}
		saOrig := sa.Clone()
		if !saOrig.Equal(sa) {
			t.Fatalf("saOrig must be equal to sa; got\n%v\nvs\n%v", saOrig, sa)
		}
		sbOrig := sb.Clone()
		if !sbOrig.Equal(sb) {
			t.Fatalf("sbOrig must be equal to sb; got\n%v\nvs\n%v", sbOrig, sb)
		}

		// Verify sa.Union(sb)
		sa.Union(sb)
		if err := expectEqual(sa, mUnion); err != nil {
			t.Fatalf("ivalid sa.Union(sb): %s", err)
		}
		if !sbOrig.Equal(sb) {
			t.Fatalf("sbOrig must be equal to sb after sa.Union(sb); got\n%v\nvs\n%v", sbOrig, sb)
		}

		// Verify sb.Union(sa)
		sa = saOrig.Clone()
		sb.Union(sa)
		if err := expectEqual(sb, mUnion); err != nil {
			t.Fatalf("invalid sb.Union(sa): %s", err)
		}
		if !saOrig.Equal(sa) {
			t.Fatalf("saOrig must be equal to sa after sb.Union(sa); got\n%v\nvs\n%v", saOrig, sa)
		}

		// Verify sa.UnionMayOwn(sb)
		sa = saOrig.Clone()
		sb = sbOrig.Clone()
		sa.UnionMayOwn(sb)
		if err := expectEqual(sa, mUnion); err != nil {
			t.Fatalf("invalid sa.UnionMayOwn(sb): %s", err)
		}
		if !sbOrig.Equal(sb) {
			t.Fatalf("sbOrig must be equal to sb after sa.UnionMayOwn(sb); got\n%v\nvs\n%v", sbOrig, sb)
		}

		// Verify sb.UnionMayOwn(sa)
		sa = saOrig.Clone()
		sb.UnionMayOwn(sa)
		if err := expectEqual(sb, mUnion); err != nil {
			t.Fatalf("invalid sb.UnionMayOwn(sa): %s", err)
		}
		if !saOrig.Equal(sa) {
			t.Fatalf("saOrig must be equal to sa after sb.UnionMayOwn(sa); got\n%v\nvs\n%v", saOrig, sa)
		}

		// Verify sa.Intersect(sb)
		sa = saOrig.Clone()
		sb = sbOrig.Clone()
		sa.Intersect(sb)
		if err := expectEqual(sa, mIntersect); err != nil {
			t.Fatalf("invalid sa.Intersect(sb): %s", err)
		}
		if !sbOrig.Equal(sb) {
			t.Fatalf("sbOrig must be equal to sb after sa.Intersect(sb); got\n%v\nvs\n%v", sbOrig, sb)
		}

		// Verify sb.Intersect(sa)
		sa = saOrig.Clone()
		sb.Intersect(sa)
		if err := expectEqual(sb, mIntersect); err != nil {
			t.Fatalf("invalid sb.Intersect(sa): %s", err)
		}
		if !saOrig.Equal(sa) {
			t.Fatalf("saOrig must be equal to sa after sb.Intersect(sa); got\n%v\nvs\n%v", saOrig, sa)
		}

		// Verify sa.Subtract(sb)
		mSubtractAB := make(map[Uint128]bool)
		for _, v := range a {
			mSubtractAB[v] = true
		}
		for _, v := range b {
			delete(mSubtractAB, v)
		}
		sa = saOrig.Clone()
		sb = sbOrig.Clone()
		sa.Subtract(sb)
		if err := expectEqual(sa, mSubtractAB); err != nil {
			t.Fatalf("invalid sa.Subtract(sb): %s", err)
		}
		if !sbOrig.Equal(sb) {
			t.Fatalf("sbOrig must be equal to sb after sa.Subtract(sb); got\n%v\nvs\n%v", sbOrig, sb)
		}

		// Verify sb.Subtract(sa)
		mSubtractBA := make(map[Uint128]bool)
		for _, v := range b {
			mSubtractBA[v] = true
		}
		for _, v := range a {
			delete(mSubtractBA, v)
		}
		sa = saOrig.Clone()
		sb.Subtract(sa)
		if err := expectEqual(sb, mSubtractBA); err != nil {
			t.Fatalf("invalid sb.Subtract(sa): %s", err)
		}
		if !saOrig.Equal(sa) {
			t.Fatalf("saOrig must be equal to sa after sb.Subtract(sa); got\n%v\nvs\n%v", saOrig, sa)
		}
	}

	f(nil, nil)
	f([]Uint128{{1, 0}}, nil)
	f([]Uint128{{1, 0}, {2, 0}, {3, 0}}, nil)
	f([]Uint128{{1, 0}, {2, 0}, {3, 0}, {1 << 16, 0}, {1 << 32, 0}, {2 << 32, 0}}, nil)
	f([]Uint128{{1, 0}}, []Uint128{{1, 0}})
	f([]Uint128{{1, 0}}, []Uint128{{1 << 16, 0}})
	f([]Uint128{{1, 0}}, []Uint128{{1 << 16, 0}})
	f([]Uint128{{1, 0}}, []Uint128{{4 << 16, 0}})
	f([]Uint128{{1, 0}}, []Uint128{{1 << 32, 0}})
	f([]Uint128{{1, 0}}, []Uint128{{1 << 32, 0}, {2 << 32, 0}})
	f([]Uint128{{1, 0}}, []Uint128{{2 << 32, 0}})
	f([]Uint128{{1, 0}, {1<<16 - 1, 0}}, []Uint128{{1 << 16, 0}})
	f([]Uint128{{0, 0}, {1<<16 - 1, 0}}, []Uint128{{1 << 16, 0}, {1<<16 - 1, 0}})
	f([]Uint128{{0, 0}, {1<<16 - 1, 0}}, []Uint128{{1 << 16, 0}, {1<<16 - 1, 0}, {2 << 16, 0}, {8 << 16, 0}})
	f([]Uint128{{0, 0}}, []Uint128{{1 << 16, 0}, {1<<16 - 1, 0}, {2 << 16, 0}, {8 << 16, 0}})
	f([]Uint128{{0, 0}, {2 << 16, 0}}, []Uint128{{1 << 16, 0}})
	f([]Uint128{{0, 0}, {2 << 16, 0}}, []Uint128{{1 << 16, 0}, {3 << 16, 0}})
	f([]Uint128{{0, 0}, {2 << 16, 0}}, []Uint128{{1 << 16, 0}, {2 << 16, 0}})
	f([]Uint128{{0, 0}, {2 << 16, 0}}, []Uint128{{1 << 16, 0}, {2 << 16, 0}, {3 << 16, 0}})
	f([]Uint128{{0, 0}, {2 << 32, 0}}, []Uint128{{1 << 32, 0}})
	f([]Uint128{{0, 0}, {2 << 32, 0}}, []Uint128{{1 << 32, 0}, {3 << 32, 0}})
	f([]Uint128{{0, 0}, {2 << 32, 0}}, []Uint128{{1 << 32, 0}, {2 << 32, 0}})
	f([]Uint128{{0, 0}, {2 << 32, 0}}, []Uint128{{1 << 32, 0}, {2 << 32, 0}, {3 << 32, 0}})

	var a []Uint128
	for i := 0; i < 100; i++ {
		a = append(a, Uint128{uint64(i), uint64(i)})
	}
	var b []Uint128
	for i := 1 << 16; i < 1<<16+1000; i++ {
		a = append(a, Uint128{uint64(i), uint64(i)})
	}
	f(a, b)

	for i := 1<<16 - 100; i < 1<<16+100; i++ {
		a = append(a, Uint128{uint64(i), uint64(i)})
	}
	for i := uint64(1) << 32; i < 1<<32+1<<16+200; i++ {
		b = append(b, Uint128{i, i})
	}
	f(a, b)

	rng := rand.New(rand.NewSource(0))
	for i := 0; i < 10; i++ {
		a = nil
		b = nil
		for j := 0; j < 1000; j++ {
			a = append(a, Uint128{uint64(rng.Intn(1e6)), uint64(rng.Intn(1e6))})
			b = append(b, Uint128{uint64(rng.Intn(1e6)), uint64(rng.Intn(1e6))})
		}
		f(a, b)
	}
}

func expectEqual(s *Set, m map[Uint128]bool) error {
	if s.Len() != len(m) {
		return fmt.Errorf("unexpected s.Len(); got %d; want %d\ns=%v\nm=%v", s.Len(), len(m), s.AppendTo(nil), m)
	}
	for _, v := range s.AppendTo(nil) {
		if !m[v] {
			return fmt.Errorf("missing value %d in m; s=%v\nm=%v", v, s.AppendTo(nil), m)
		}
	}

	// Additional check via s.Has()
	for v := range m {
		if !s.Has(v) {
			return fmt.Errorf("missing value %d in s; s=%v\nm=%v", v, s.AppendTo(nil), m)
		}
	}

	// Extra check via s.ForEach()
	var err error
	s.ForEach(func(part []Uint128) bool {
		for _, v := range part {
			if !m[v] {
				err = fmt.Errorf("missing value %d in m inside s.ForEach; s=%v\nm=%v", v, s.AppendTo(nil), m)
				return false
			}
		}
		return true
	})
	return err
}
