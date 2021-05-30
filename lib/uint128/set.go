package uint128

import (
	"sort"
	"sync"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/uint64set"
)

type bucket64Sorter []bucket64

func (s *bucket64Sorter) Len() int { return len(*s) }
func (s *bucket64Sorter) Less(i, j int) bool {
	a := *s
	return a[i].hi < a[j].hi
}
func (s *bucket64Sorter) Swap(i, j int) {
	a := *s
	a[i], a[j] = a[j], a[i]
}

type bucket64 struct {
	hi uint64

	set *uint64set.Set
}

func (b *bucket64) copyTo(dst *bucket64) {
	dst.hi = b.hi
	dst.set = b.set.Clone()
}

type Set struct {
	itemsCount int
	buckets    bucket64Sorter
}

func (s *Set) Len() int {
	if s == nil {
		return 0
	}
	return s.itemsCount
}

func (s *Set) SizeBytes() uint64 {
	if s == nil {
		return 0
	}
	n := uint64(unsafe.Sizeof(*s))
	for i := range s.buckets {
		b64 := &s.buckets[i]
		n += uint64(unsafe.Sizeof(b64))
		n += b64.set.SizeBytes()
	}
	return n
}

func (s *Set) Add(x Uint128) {
	bs := s.buckets
	if len(bs) > 0 && bs[0].hi == x.Hi {
		// Manually inline bucket32.add for performance reasons.
		b64 := bs[0]
		b64Len := b64.set.Len()
		b64.set.Add(x.Lo)
		s.itemsCount += b64.set.Len() - b64Len
		return
	}
	for i := range bs {
		b64 := &bs[i]
		if b64.hi == x.Hi {
			b64Len := b64.set.Len()
			b64.set.Add(x.Lo)
			s.itemsCount += b64.set.Len() - b64Len
			return
		}
	}
	b64 := s.addBucket64()
	b64.hi = x.Hi
	b64.set = &uint64set.Set{}

	b64Len := b64.set.Len()
	b64.set.Add(x.Lo)
	s.itemsCount += b64.set.Len() - b64Len
}

func (s *Set) Has(x Uint128) bool {
	if s == nil {
		return false
	}
	bs := s.buckets
	for i := range bs {
		b64 := &bs[i]
		if b64.hi == x.Hi {
			return b64.set.Has(x.Lo)
		}
	}
	return false
}

// Del deletes x from s.
func (s *Set) Del(x Uint128) {
	bs := s.buckets
	if len(bs) > 0 && bs[0].hi == x.Hi {
		b64 := bs[0]
		b64Len := b64.set.Len()
		b64.set.Del(x.Lo)
		s.itemsCount -= b64Len - b64.set.Len()
		return
	}
	for i := range bs {
		b64 := &bs[i]
		if b64.hi == x.Hi {
			b64Len := b64.set.Len()
			b64.set.Del(x.Lo)
			s.itemsCount -= b64Len - b64.set.Len()
			return
		}
	}
}

func (s *Set) addBucket64() *bucket64 {
	s.buckets = append(s.buckets, bucket64{set: &uint64set.Set{}})
	return &s.buckets[len(s.buckets)-1]
}

// Clone returns an independent copy of s.
func (s *Set) Clone() *Set {
	if s == nil || s.itemsCount == 0 {
		// Return an empty set, so data could be added into it later.
		return &Set{}
	}
	var dst Set
	dst.itemsCount = s.itemsCount
	dst.buckets = make([]bucket64, len(s.buckets))
	for i := range s.buckets {
		s.buckets[i].copyTo(&dst.buckets[i])
	}
	return &dst
}

func (s *Set) fixItemsCount() {
	n := 0
	for i := range s.buckets {
		n += s.buckets[i].set.Len()
	}
	s.itemsCount = n
}

func (s *Set) cloneShallow() *Set {
	var dst Set
	dst.itemsCount = s.itemsCount
	dst.buckets = append(dst.buckets[:0], s.buckets...)
	return &dst
}

func (s *Set) ForEach(f func(part []Uint128) bool) {
	if s == nil {
		return
	}
	var isContinue bool
	for i := range s.buckets {
		hi := s.buckets[i].hi
		s.buckets[i].set.ForEach(func(part []uint64) bool {
			b128s := make([]Uint128, 0, len(part))
			for _, lo := range part {
				b128s = append(b128s, Uint128{
					Lo: lo,
					Hi: hi,
				})
			}
			isContinue = f(b128s)
			return isContinue
		})
		if !isContinue {
			return
		}
	}
}

func (s *Set) AddMulti(a []Uint128) {
	if len(a) == 0 {
		return
	}
	slowPath := false
	hi := a[0].Hi
	for _, x := range a[1:] {
		if hi != x.Hi {
			slowPath = true
			break
		}
	}
	if slowPath {
		for _, x := range a {
			s.Add(x)
		}
		return
	}

	// Fast path
	bs := s.buckets
	var b64 *bucket64
	for i := range bs {
		if bs[i].hi == hi {
			b64 = &bs[i]
			break
		}
	}
	if b64 == nil {
		b64 = s.addBucket64()
		b64.hi = hi
	}
	los := getU64s()
	for i := range a {
		los = append(los, a[i].Lo)
	}
	b64Len := b64.set.Len()
	b64.set.AddMulti(los)
	s.itemsCount += b64.set.Len() - b64Len
	putU64s(los)
}

func (s *Set) sort() {
	// sort s.buckets if it isn't sorted yet
	if !sort.IsSorted(&s.buckets) {
		sort.Sort(&s.buckets)
	}
}

func (s *Set) Union(a *Set) {
	s.union(a, false)
}

func (s *Set) UnionMayOwn(a *Set) {
	s.union(a, true)
}

func (s *Set) union(a *Set, mayOwn bool) {
	if a.Len() == 0 {
		// Fast path - nothing to union.
		return
	}
	if s.Len() == 0 {
		// Fast path - copy `a` to `s`.
		if !mayOwn {
			a = a.Clone()
		}
		*s = *a
		return
	}
	// Make shallow copy of `a`, since it can be modified by a.sort().
	if !mayOwn {
		a = a.cloneShallow()
	}
	a.sort()
	s.sort()
	i := 0
	j := 0
	sBucketsLen := len(s.buckets)
	for {
		for i < sBucketsLen && j < len(a.buckets) && s.buckets[i].hi < a.buckets[j].hi {
			i++
		}
		if i >= sBucketsLen {
			for j < len(a.buckets) {
				b64 := s.addBucket64()
				a.buckets[j].copyTo(b64)
				j++
			}
			break
		}
		for j < len(a.buckets) && a.buckets[j].hi < s.buckets[i].hi {
			b64 := s.addBucket64()
			a.buckets[j].copyTo(b64)
			j++
		}
		if j >= len(a.buckets) {
			break
		}
		if s.buckets[i].hi == a.buckets[j].hi {
			if mayOwn {
				s.buckets[i].set.UnionMayOwn(a.buckets[j].set)
			} else {
				s.buckets[i].set.Union(a.buckets[j].set)
			}
			i++
			j++
		}
	}
	s.fixItemsCount()
}

// Equal returns true if s contains the same items as a.
func (s *Set) Equal(a *Set) bool {
	if s.Len() != a.Len() {
		return false
	}
	equal := true
	a.ForEach(func(part []Uint128) bool {
		for _, x := range part {
			if !s.Has(x) {
				equal = false
				return false
			}
		}
		return true
	})
	return equal
}

func (s *Set) Subtract(a *Set) {
	if s.Len() == 0 || a.Len() == 0 {
		// Fast path - nothing to subtract.
		return
	}
	a.ForEach(func(part []Uint128) bool {
		for _, x := range part {
			s.Del(x)
		}
		return true
	})
}

func (s *Set) Intersect(a *Set) {
	if s.Len() == 0 || a.Len() == 0 {
		// Fast path - the result is empty.
		*s = Set{}
		return
	}
	// Make shallow copy of `a`, since it can be modified by a.sort().
	a = a.cloneShallow()
	a.sort()
	s.sort()
	i := 0
	j := 0
	for {
		for i < len(s.buckets) && j < len(a.buckets) && s.buckets[i].hi < a.buckets[j].hi {
			s.buckets[i] = bucket64{}
			i++
		}
		if i >= len(s.buckets) {
			break
		}
		for j < len(a.buckets) && a.buckets[j].hi < s.buckets[i].hi {
			j++
		}
		if j >= len(a.buckets) {
			for i < len(s.buckets) {
				s.buckets[i] = bucket64{}
				i++
			}
			break
		}
		if s.buckets[i].hi == a.buckets[j].hi {
			s.buckets[i].set.Intersect(a.buckets[j].set)
			i++
			j++
		}
	}
	s.fixItemsCount()
}

func (s *Set) AppendTo(dst []Uint128) []Uint128 {
	if s.Len() == 0 {
		return dst
	}

	// pre-allocate memory for dst
	dstLen := len(dst)
	if n := s.Len() - cap(dst) + dstLen; n > 0 {
		dst = append(dst[:cap(dst)], make([]Uint128, n)...)
		dst = dst[:dstLen]
	}
	s.sort()
	los := getU64s()
	for i := range s.buckets {
		los = s.buckets[i].set.AppendTo(los[:0])
		for j := range los {
			dst = append(dst, Uint128{Hi: s.buckets[i].hi, Lo: los[j]})
		}
	}
	putU64s(los)
	return dst
}

func getU64s() []uint64 {
	los := u64sPool.Get()
	if los == nil {
		return make([]uint64, 0)
	}
	return los.([]uint64)
}

func putU64s(los []uint64) {
	los = los[:0]
	u64sPool.Put(los)
}

var u64sPool sync.Pool
