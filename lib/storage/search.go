package storage

import (
	"fmt"
	"io"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storagepacelimiter"
)

// BlockRef references a Block.
//
// BlockRef is valid only until the corresponding Search is valid,
// i.e. it becomes invalid after Search.MustClose is called.
type BlockRef struct {
	p  *part
	bh blockHeader
}

func (br *BlockRef) reset() {
	br.p = nil
	br.bh = blockHeader{}
}

func (br *BlockRef) init(p *part, bh *blockHeader) {
	br.p = p
	br.bh = *bh
}

// MustReadBlock reads block from br to dst.
//
// if fetchData is false, then only block header is read, otherwise all the data is read.
func (br *BlockRef) MustReadBlock(dst *Block, fetchData uint8) {
	dst.Reset()
	dst.bh = br.bh

	switch fetchData {
	case OnlyFetchTime:
		dst.timestampsData = bytesutil.Resize(dst.timestampsData[:0], int(br.bh.TimestampsBlockSize))
		br.p.timestampsFile.MustReadAt(dst.timestampsData, int64(br.bh.TimestampsBlockOffset))

	case FetchAll:
		dst.timestampsData = bytesutil.Resize(dst.timestampsData[:0], int(br.bh.TimestampsBlockSize))
		br.p.timestampsFile.MustReadAt(dst.timestampsData, int64(br.bh.TimestampsBlockOffset))

		dst.valuesData = bytesutil.Resize(dst.valuesData[:0], int(br.bh.ValuesBlockSize))
		br.p.valuesFile.MustReadAt(dst.valuesData, int64(br.bh.ValuesBlockOffset))
	}
}

// MarshalBlock marshals b to dst.
//
// b.MarshalData must be called on b before calling MarshalBlock.
func MarshalBlock(dst []byte, b *Block) []byte {
	dst = b.bh.Marshal(dst)
	dst = encoding.MarshalBytes(dst, b.timestampsData)
	dst = encoding.MarshalBytes(dst, b.valuesData)
	return dst
}

// UnmarshalBlock unmarshal Block from src to dst.
//
// dst.UnmarshalData isn't called on the block.
func UnmarshalBlock(dst *Block, src []byte) ([]byte, error) {
	tail, err := dst.bh.Unmarshal(src)
	if err != nil {
		return tail, fmt.Errorf("cannot unmarshal blockHeader: %w", err)
	}
	src = tail

	tail, tds, err := encoding.UnmarshalBytes(src)
	if err != nil {
		return tail, fmt.Errorf("cannot unmarshal timestampsData: %w", err)
	}
	dst.timestampsData = append(dst.timestampsData[:0], tds...)
	src = tail

	tail, vd, err := encoding.UnmarshalBytes(src)
	if err != nil {
		return tail, fmt.Errorf("cannot unmarshal valuesData: %w", err)
	}
	dst.valuesData = append(dst.valuesData[:0], vd...)
	src = tail

	return src, nil
}

// Search is a search for time series.
type Search struct {
	// BlockRef is updated with each Search.NextMetricBlock call.
	BlockRef *BlockRef

	storage *Storage

	ts tableSearch

	// tr contains time range used in the serach.
	tr ScanRange

	// tfss contains tag filters used in the search.
	tfss []*TagFilters

	// deadline in unix timestamp seconds for the current search.
	deadline uint64

	err error

	needClosing bool

	loops int
}

func (s *Search) reset() {
	s.BlockRef = nil

	s.storage = nil
	s.ts.reset()
	s.tr = ScanRange{}
	s.tfss = nil
	s.deadline = 0
	s.err = nil
	s.needClosing = false
	s.loops = 0
}

// Init initializes s from the given storage, tfss and tr.
//
// MustClose must be called when the search is done.
//
// Init returns the upper bound on the number of found time series.
func (s *Search) Init(storage *Storage, tfss []*TagFilters, tr ScanRange, deadline uint64) int {
	if s.needClosing {
		logger.Panicf("BUG: missing MustClose call before the next call to Init")
	}

	s.reset()
	s.tr = tr
	s.tfss = tfss
	s.deadline = deadline
	s.needClosing = true

	tsids, err := storage.searchTraceIDs(tfss, tr, deadline)
	// It is ok to call Init on error from storage.searchTraceIDs.
	// Init must be called before returning because it will fail
	// on Seach.MustClose otherwise.
	s.ts.Init(storage.tb, tsids, tr)

	if err != nil {
		s.err = err
		return 0
	}

	s.storage = storage
	return len(tsids)
}

// MustClose closes the Search.
func (s *Search) MustClose() {
	if !s.needClosing {
		logger.Panicf("BUG: missing Init call before MustClose")
	}
	s.ts.MustClose()
	s.reset()
}

// Error returns the last error from s.
func (s *Search) Error() error {
	if s.err == io.EOF || s.err == nil {
		return nil
	}
	return fmt.Errorf("error when searching for tagFilters=%s on the time range %s: %w", s.tfss, s.tr.String(), s.err)
}

// NextMetricBlock proceeds to the next MetricBlockRef.
func (s *Search) NextMetricBlock() bool {
	if s.err != nil {
		return false
	}
	for s.ts.NextBlock() {
		if s.loops&paceLimiterSlowIterationsMask == 0 {
			if err := checkSearchDeadlineAndPace(s.deadline); err != nil {
				s.err = err
				return false
			}
		}
		s.loops++
		s.BlockRef = s.ts.BlockRef
		return true
	}
	if err := s.ts.Error(); err != nil {
		s.err = err
		return false
	}

	s.err = io.EOF
	return false
}

type FetchDataOption = uint8

const (
	NotFetch      FetchDataOption = 1
	OnlyFetchTime                 = iota
	FetchAll
)

// SearchQuery is used for sending search queries from vmselect to vmstorage.
type SearchQuery struct {
	MinTimestamp int64
	MaxTimestamp int64
	TagFilterss  [][]TagFilter
	Limit        int
	Forward      bool
	FetchData    FetchDataOption
}

// TagFilter represents a single tag filter from SearchQuery.
type TagFilter struct {
	Key        []byte
	Value      []byte
	IsNegative bool
	IsRegexp   bool
}

// String returns string representation of tf.
func (tf *TagFilter) String() string {
	var bb bytesutil.ByteBuffer
	fmt.Fprintf(&bb, "{Key=%q, Value=%q, IsNegative: %v, IsRegexp: %v}", tf.Key, tf.Value, tf.IsNegative, tf.IsRegexp)
	return string(bb.B)
}

// Marshal appends marshaled tf to dst and returns the result.
func (tf *TagFilter) Marshal(dst []byte) []byte {
	dst = encoding.MarshalBytes(dst, tf.Key)
	dst = encoding.MarshalBytes(dst, tf.Value)

	x := 0
	if tf.IsNegative {
		x = 2
	}
	if tf.IsRegexp {
		x |= 1
	}
	dst = append(dst, byte(x))

	return dst
}

// Unmarshal unmarshals tf from src and returns the tail.
func (tf *TagFilter) Unmarshal(src []byte) ([]byte, error) {
	tail, k, err := encoding.UnmarshalBytes(src)
	if err != nil {
		return tail, fmt.Errorf("cannot unmarshal Key: %w", err)
	}
	tf.Key = append(tf.Key[:0], k...)
	src = tail

	tail, v, err := encoding.UnmarshalBytes(src)
	if err != nil {
		return tail, fmt.Errorf("cannot unmarshal Value: %w", err)
	}
	tf.Value = append(tf.Value[:0], v...)
	src = tail

	if len(src) < 1 {
		return src, fmt.Errorf("cannot unmarshal IsNegative+IsRegexp from empty src")
	}
	x := src[0]
	switch x {
	case 0:
		tf.IsNegative = false
		tf.IsRegexp = false
	case 1:
		tf.IsNegative = false
		tf.IsRegexp = true
	case 2:
		tf.IsNegative = true
		tf.IsRegexp = false
	case 3:
		tf.IsNegative = true
		tf.IsRegexp = true
	default:
		return src, fmt.Errorf("unexpected value for IsNegative+IsRegexp: %d; must be in the range [0..3]", x)
	}
	src = src[1:]

	return src, nil
}

// String returns string representation of the search query.
func (sq *SearchQuery) String() string {
	var bb bytesutil.ByteBuffer
	fmt.Fprintf(&bb, "MinTimestamp=%s, MaxTimestamp=%s, TagFilters=[\n", timestampToTime(sq.MinTimestamp), timestampToTime(sq.MaxTimestamp))
	for _, tagFilters := range sq.TagFilterss {
		for _, tf := range tagFilters {
			fmt.Fprintf(&bb, "%s", tf.String())
		}
		fmt.Fprintf(&bb, "\n")
	}
	fmt.Fprintf(&bb, "]")
	return string(bb.B)
}

// Marshal appends marshaled sq to dst and returns the result.
func (sq *SearchQuery) Marshal(dst []byte) []byte {
	dst = encoding.MarshalVarInt64(dst, sq.MinTimestamp)
	dst = encoding.MarshalVarInt64(dst, sq.MaxTimestamp)
	dst = encoding.MarshalVarUint64(dst, uint64(len(sq.TagFilterss)))
	for _, tagFilters := range sq.TagFilterss {
		dst = encoding.MarshalVarUint64(dst, uint64(len(tagFilters)))
		for i := range tagFilters {
			dst = tagFilters[i].Marshal(dst)
		}
	}
	dst = encoding.MarshalVarInt64(dst, int64(sq.Limit))

	switch sq.Forward {
	case false:
		dst = append(dst, sq.FetchData)
	case true:
		dst = append(dst, FetchAll+sq.FetchData)
	}
	return dst
}

// Unmarshal unmarshals sq from src and returns the tail.
func (sq *SearchQuery) Unmarshal(src []byte) ([]byte, error) {
	tail, minTs, err := encoding.UnmarshalVarInt64(src)
	if err != nil {
		return src, fmt.Errorf("cannot unmarshal MinTimestamp: %w", err)
	}
	sq.MinTimestamp = minTs
	src = tail

	tail, maxTs, err := encoding.UnmarshalVarInt64(src)
	if err != nil {
		return src, fmt.Errorf("cannot unmarshal MaxTimestamp: %w", err)
	}
	sq.MaxTimestamp = maxTs
	src = tail

	tail, tfssCount, err := encoding.UnmarshalVarUint64(src)
	if err != nil {
		return src, fmt.Errorf("cannot unmarshal the count of TagFilterss: %w", err)
	}
	if n := int(tfssCount) - cap(sq.TagFilterss); n > 0 {
		sq.TagFilterss = append(sq.TagFilterss[:cap(sq.TagFilterss)], make([][]TagFilter, n)...)
	}
	sq.TagFilterss = sq.TagFilterss[:tfssCount]
	src = tail

	for i := 0; i < int(tfssCount); i++ {
		tail, tfsCount, err := encoding.UnmarshalVarUint64(src)
		if err != nil {
			return src, fmt.Errorf("cannot unmarshal the count of TagFilters: %w", err)
		}
		src = tail

		tagFilters := sq.TagFilterss[i]
		if n := int(tfsCount) - cap(tagFilters); n > 0 {
			tagFilters = append(tagFilters[:cap(tagFilters)], make([]TagFilter, n)...)
		}
		tagFilters = tagFilters[:tfsCount]
		for j := 0; j < int(tfsCount); j++ {
			tail, err := tagFilters[j].Unmarshal(src)
			if err != nil {
				return tail, fmt.Errorf("cannot unmarshal TagFilter #%d: %w", j, err)
			}
			src = tail
		}
		sq.TagFilterss[i] = tagFilters
	}

	tail, limit, err := encoding.UnmarshalVarInt64(src)
	if err != nil {
		return src, fmt.Errorf("cannot unmarshal limit: %w", err)
	}
	sq.Limit = int(limit)
	src = tail

	x := src[0]
	if x <= FetchAll {
		sq.Forward = false
		sq.FetchData = x
	} else {
		sq.Forward = true
		sq.FetchData = x - FetchAll
	}
	src = src[1:]

	return src, nil
}

func checkSearchDeadlineAndPace(deadline uint64) error {
	if fasttime.UnixTimestamp() > deadline {
		return ErrDeadlineExceeded
	}
	storagepacelimiter.Search.WaitIfNeeded()
	return nil
}

const (
	paceLimiterFastIterationsMask   = 1<<16 - 1
	paceLimiterMediumIterationsMask = 1<<14 - 1
	paceLimiterSlowIterationsMask   = 1<<12 - 1
)
