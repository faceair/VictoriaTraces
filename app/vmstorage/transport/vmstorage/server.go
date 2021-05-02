package vmstorage

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/consts"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/handshake"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/netutil"
	"github.com/VictoriaMetrics/metrics"
	"github.com/faceair/VictoriaTraces/lib/storage"
)

var (
	maxTagKeysPerSearch   = flag.Int("search.maxTagKeys", 100e3, "The maximum number of tag keys returned per search")
	maxTagValuesPerSearch = flag.Int("search.maxTagValues", 100e3, "The maximum number of tag values returned per search")

	disableRPCCompression = flag.Bool(`rpc.disableCompression`, false, "Disable compression of RPC traffic. This reduces CPU usage at the cost of higher network bandwidth usage")

	denyQueriesOutsideRetention = flag.Bool("denyQueriesOutsideRetention", false, "Whether to deny queries outside of the configured -retentionPeriod. "+
		"When set, then /api/v1/query_range would return '503 Service Unavailable' error for queries with 'from' value outside -retentionPeriod. "+
		"This may be useful when multiple data sources with distinct retentions are hidden behind query-tee")
)

// Server processes connections from vminsert and vmselect.
type Server struct {
	// Move stopFlag to the top of the struct in order to fix atomic access to it on 32-bit arches.
	// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/212
	stopFlag uint64

	storage *storage.Storage

	vminsertLN net.Listener
	vmselectLN net.Listener

	vminsertWG sync.WaitGroup
	vmselectWG sync.WaitGroup

	vminsertConnsMap connsMap
	vmselectConnsMap connsMap
}

type connsMap struct {
	mu       sync.Mutex
	m        map[net.Conn]struct{}
	isClosed bool
}

func (cm *connsMap) Init() {
	cm.m = make(map[net.Conn]struct{})
	cm.isClosed = false
}

func (cm *connsMap) Add(c net.Conn) bool {
	cm.mu.Lock()
	ok := !cm.isClosed
	if ok {
		cm.m[c] = struct{}{}
	}
	cm.mu.Unlock()
	return ok
}

func (cm *connsMap) Delete(c net.Conn) {
	cm.mu.Lock()
	delete(cm.m, c)
	cm.mu.Unlock()
}

func (cm *connsMap) CloseAll() {
	cm.mu.Lock()
	for c := range cm.m {
		_ = c.Close()
	}
	cm.isClosed = true
	cm.mu.Unlock()
}

// NewServer returns new Server.
func NewServer(vminsertAddr, vmselectAddr string, storage *storage.Storage) (*Server, error) {
	vminsertLN, err := netutil.NewTCPListener("vminsert", vminsertAddr)
	if err != nil {
		return nil, fmt.Errorf("unable to listen vminsertAddr %s: %w", vminsertAddr, err)
	}
	vmselectLN, err := netutil.NewTCPListener("vmselect", vmselectAddr)
	if err != nil {
		return nil, fmt.Errorf("unable to listen vmselectAddr %s: %w", vmselectAddr, err)
	}
	s := &Server{
		storage: storage,

		vminsertLN: vminsertLN,
		vmselectLN: vmselectLN,
	}
	s.vminsertConnsMap.Init()
	s.vmselectConnsMap.Init()
	return s, nil
}

// RunVMInsert runs a server accepting connections from vminsert.
func (s *Server) RunVMInsert() {
	logger.Infof("accepting vminsert conns at %s", s.vminsertLN.Addr())
	for {
		c, err := s.vminsertLN.Accept()
		if err != nil {
			if pe, ok := err.(net.Error); ok && pe.Temporary() {
				continue
			}
			if s.isStopping() {
				return
			}
			logger.Panicf("FATAL: cannot process vminsert conns at %s: %s", s.vminsertLN.Addr(), err)
		}
		logger.Infof("accepted vminsert conn from %s", c.RemoteAddr())

		if !s.vminsertConnsMap.Add(c) {
			// The server is closed.
			_ = c.Close()
			return
		}
		vminsertConns.Inc()
		s.vminsertWG.Add(1)
		go func() {
			defer func() {
				s.vminsertConnsMap.Delete(c)
				vminsertConns.Dec()
				s.vminsertWG.Done()
			}()

			// There is no need in response compression, since
			// vmstorage sends only small packets to vminsert.
			compressionLevel := 0
			bc, err := handshake.VMInsertServer(c, compressionLevel)
			if err != nil {
				if s.isStopping() {
					// c is stopped inside Server.MustClose
					return
				}
				logger.Errorf("cannot perform vminsert handshake with client %q: %s", c.RemoteAddr(), err)
				_ = c.Close()
				return
			}
			defer func() {
				if !s.isStopping() {
					logger.Infof("closing vminsert conn from %s", c.RemoteAddr())
				}
				_ = bc.Close()
			}()

			logger.Infof("processing vminsert conn from %s", c.RemoteAddr())
			if err := s.processVMInsertConn(bc); err != nil {
				if s.isStopping() {
					return
				}
				vminsertConnErrors.Inc()
				logger.Errorf("cannot process vminsert conn from %s: %s", c.RemoteAddr(), err)
			}
		}()
	}
}

var (
	vminsertConns      = metrics.NewCounter("vm_vminsert_conns")
	vminsertConnErrors = metrics.NewCounter("vm_vminsert_conn_errors_total")
)

// RunVMSelect runs a server accepting connections from vmselect.
func (s *Server) RunVMSelect() {
	logger.Infof("accepting vmselect conns at %s", s.vmselectLN.Addr())
	for {
		c, err := s.vmselectLN.Accept()
		if err != nil {
			if pe, ok := err.(net.Error); ok && pe.Temporary() {
				continue
			}
			if s.isStopping() {
				return
			}
			logger.Panicf("FATAL: cannot process vmselect conns at %s: %s", s.vmselectLN.Addr(), err)
		}
		logger.Infof("accepted vmselect conn from %s", c.RemoteAddr())

		if !s.vmselectConnsMap.Add(c) {
			// The server is closed.
			_ = c.Close()
			return
		}
		vmselectConns.Inc()
		s.vmselectWG.Add(1)
		go func() {
			defer func() {
				s.vmselectConnsMap.Delete(c)
				vmselectConns.Dec()
				s.vmselectWG.Done()
			}()

			// Compress responses to vmselect even if they already contain compressed blocks.
			// Responses contain uncompressed metric names, which should compress well
			// when the response contains high number of time series.
			// Additionally, recently added metric blocks are usually uncompressed, so the compression
			// should save network bandwidth.
			compressionLevel := 1
			if *disableRPCCompression {
				compressionLevel = 0
			}
			bc, err := handshake.VMSelectServer(c, compressionLevel)
			if err != nil {
				if s.isStopping() {
					// c is closed inside Server.MustClose
					return
				}
				logger.Errorf("cannot perform vmselect handshake with client %q: %s", c.RemoteAddr(), err)
				_ = c.Close()
				return
			}

			defer func() {
				if !s.isStopping() {
					logger.Infof("closing vmselect conn from %s", c.RemoteAddr())
				}
				_ = bc.Close()
			}()

			logger.Infof("processing vmselect conn from %s", c.RemoteAddr())
			if err := s.processVMSelectConn(bc); err != nil {
				if s.isStopping() {
					return
				}
				vmselectConnErrors.Inc()
				logger.Errorf("cannot process vmselect conn %s: %s", c.RemoteAddr(), err)
			}
		}()
	}
}

var (
	vmselectConns      = metrics.NewCounter("vm_vmselect_conns")
	vmselectConnErrors = metrics.NewCounter("vm_vmselect_conn_errors_total")
)

// MustClose gracefully closes the server,
// so it no longer touches s.storage after returning.
func (s *Server) MustClose() {
	// Mark the server as stoping.
	s.setIsStopping()

	// Stop accepting new connections from vminsert and vmselect.
	if err := s.vminsertLN.Close(); err != nil {
		logger.Panicf("FATAL: cannot close vminsert listener: %s", err)
	}
	if err := s.vmselectLN.Close(); err != nil {
		logger.Panicf("FATAL: cannot close vmselect listener: %s", err)
	}

	// Close existing connections from vminsert, so the goroutines
	// processing these connections are finished.
	s.vminsertConnsMap.CloseAll()

	// Close existing connections from vmselect, so the goroutines
	// processing these connections are finished.
	s.vmselectConnsMap.CloseAll()

	// Wait until all the goroutines processing vminsert and vmselect conns
	// are finished.
	s.vminsertWG.Wait()
	s.vmselectWG.Wait()
}

func (s *Server) setIsStopping() {
	atomic.StoreUint64(&s.stopFlag, 1)
}

func (s *Server) isStopping() bool {
	return atomic.LoadUint64(&s.stopFlag) != 0
}

func (s *Server) processVMInsertConn(bc *handshake.BufferedConn) error {
	sizeBuf := make([]byte, 8)
	var reqBuf []byte
	remoteAddr := bc.RemoteAddr().String()
	for {
		if _, err := io.ReadFull(bc, sizeBuf); err != nil {
			if err == io.EOF {
				// Remote end gracefully closed the connection.
				return nil
			}
			return fmt.Errorf("cannot read packet size: %w", err)
		}
		packetSize := encoding.UnmarshalUint64(sizeBuf)
		if packetSize > consts.MaxInsertPacketSize {
			return fmt.Errorf("too big packet size: %d; shouldn't exceed %d", packetSize, consts.MaxInsertPacketSize)
		}
		reqBuf = bytesutil.Resize(reqBuf, int(packetSize))
		if n, err := io.ReadFull(bc, reqBuf); err != nil {
			return fmt.Errorf("cannot read packet with size %d: %w; read only %d bytes", packetSize, err, n)
		}
		// Send `ack` to vminsert that the packet has been received.
		deadline := time.Now().Add(5 * time.Second)
		if err := bc.SetWriteDeadline(deadline); err != nil {
			return fmt.Errorf("cannot set write deadline for sending `ack` to vminsert: %w", err)
		}
		sizeBuf[0] = 1
		if _, err := bc.Write(sizeBuf[:1]); err != nil {
			return fmt.Errorf("cannot send `ack` to vminsert: %w", err)
		}
		if err := bc.Flush(); err != nil {
			return fmt.Errorf("cannot flush `ack` to vminsert: %w", err)
		}
		vminsertPacketsRead.Inc()

		uw := getUnmarshalWork()
		uw.storage = s.storage
		uw.remoteAddr = remoteAddr
		uw.reqBuf, reqBuf = reqBuf, uw.reqBuf
		unmarshalWorkCh <- uw
	}
}

var (
	vminsertPacketsRead = metrics.NewCounter("vm_vminsert_packets_read_total")
	vminsertMetricsRead = metrics.NewCounter("vm_vminsert_metrics_read_total")
)

func getUnmarshalWork() *unmarshalWork {
	v := unmarshalWorkPool.Get()
	if v == nil {
		return &unmarshalWork{}
	}
	return v.(*unmarshalWork)
}

// StartUnmarshalWorkers starts workers for unmarshaling data obtained from vminsert connections.
//
// This function must be called before servers are created via NewServer.
func StartUnmarshalWorkers() {
	gomaxprocs := runtime.GOMAXPROCS(-1)
	unmarshalWorkCh = make(chan *unmarshalWork, gomaxprocs)
	unmarshalWorkersWG.Add(gomaxprocs)
	for i := 0; i < gomaxprocs; i++ {
		go func() {
			defer unmarshalWorkersWG.Done()
			for uw := range unmarshalWorkCh {
				uw.Unmarshal()
				putUnmarshalWork(uw)
			}
		}()
	}
}

// StopUnmarshalWorkers stops unmarshal workers which were started with StartUnmarshalWorkers.
//
// This function must be called after Server.MustClose().
func StopUnmarshalWorkers() {
	close(unmarshalWorkCh)
	unmarshalWorkersWG.Wait()
}

var (
	unmarshalWorkCh    chan *unmarshalWork
	unmarshalWorkersWG sync.WaitGroup
)

func putUnmarshalWork(uw *unmarshalWork) {
	uw.reset()
	unmarshalWorkPool.Put(uw)
}

var unmarshalWorkPool sync.Pool

type unmarshalWork struct {
	storage       *storage.Storage
	remoteAddr    string
	mrs           []storage.SpanRow
	reqBuf        []byte
	lastResetTime uint64
}

func (uw *unmarshalWork) reset() {
	if (len(uw.mrs)*4 > cap(uw.mrs) || len(uw.reqBuf)*4 > cap(uw.reqBuf)) && fasttime.UnixTimestamp()-uw.lastResetTime > 10 {
		// Periodically reset mrs and reqBuf in order to prevent from gradual memory usage growth
		// when ceratin entries in mr contain too long labels.
		// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/490 for details.
		uw.mrs = nil
		uw.reqBuf = nil
		uw.lastResetTime = fasttime.UnixTimestamp()
	}
	uw.storage = nil
	uw.remoteAddr = ""
	uw.mrs = uw.mrs[:0]
	uw.reqBuf = uw.reqBuf[:0]
}

func (uw *unmarshalWork) Unmarshal() {
	mrs := uw.mrs[:0]
	tail := uw.reqBuf
	for len(tail) > 0 {
		if len(mrs) < cap(mrs) {
			mrs = mrs[:len(mrs)+1]
		} else {
			mrs = append(mrs, storage.SpanRow{})
		}
		mr := &mrs[len(mrs)-1]
		var err error
		tail, err = mr.Unmarshal(tail)
		if err != nil {
			logger.Errorf("cannot unmarshal SpanRow obtained from %s: %s", uw.remoteAddr, err)
			uw.mrs = mrs[:0]
			return
		}
		if len(mrs) >= 10000 {
			// Store the collected mrs in order to reduce memory usage
			// when too big number of mrs are sent in each packet.
			// This should help with https://github.com/VictoriaMetrics/VictoriaMetrics/issues/490
			uw.mrs = mrs
			uw.flushRows()
			mrs = uw.mrs[:0]
		}
	}
	uw.mrs = mrs
	uw.flushRows()
}

func (uw *unmarshalWork) flushRows() {
	vminsertMetricsRead.Add(len(uw.mrs))
	err := uw.storage.AddRows(uw.mrs)
	uw.mrs = uw.mrs[:0]
	if err != nil {
		logger.Errorf("cannot store metrics obtained from %s: %s", uw.remoteAddr, err)
	}
}

func (s *Server) processVMSelectConn(bc *handshake.BufferedConn) error {
	ctx := &vmselectRequestCtx{
		bc:      bc,
		sizeBuf: make([]byte, 8),
	}
	for {
		if err := s.processVMSelectRequest(ctx); err != nil {
			if err == io.EOF {
				// Remote client gracefully closed the connection.
				return nil
			}
			if errors.Is(err, storage.ErrDeadlineExceeded) {
				return fmt.Errorf("cannot process vmselect request in %d seconds: %w", ctx.timeout, err)
			}
			return fmt.Errorf("cannot process vmselect request: %w", err)
		}
		if err := bc.Flush(); err != nil {
			return fmt.Errorf("cannot flush compressed buffers: %w", err)
		}
	}
}

type vmselectRequestCtx struct {
	bc      *handshake.BufferedConn
	sizeBuf []byte
	dataBuf []byte

	sq   storage.SearchQuery
	tfss []*storage.TagFilters
	sr   storage.Search
	mb   storage.Block

	// timeout in seconds for the current request
	timeout uint64

	// deadline in unix timestamp seconds for the current request.
	deadline uint64
}

func (ctx *vmselectRequestCtx) readUint32() (uint32, error) {
	ctx.sizeBuf = bytesutil.Resize(ctx.sizeBuf, 4)
	if _, err := io.ReadFull(ctx.bc, ctx.sizeBuf); err != nil {
		if err == io.EOF {
			return 0, err
		}
		return 0, fmt.Errorf("cannot read uint32: %w", err)
	}
	n := encoding.UnmarshalUint32(ctx.sizeBuf)
	return n, nil
}

func (ctx *vmselectRequestCtx) readUint64() (uint64, error) {
	ctx.sizeBuf = bytesutil.Resize(ctx.sizeBuf, 8)
	if _, err := io.ReadFull(ctx.bc, ctx.sizeBuf); err != nil {
		if err == io.EOF {
			return 0, err
		}
		return 0, fmt.Errorf("cannot read uint64: %w", err)
	}
	n := encoding.UnmarshalUint64(ctx.sizeBuf)
	return n, nil
}

func (ctx *vmselectRequestCtx) readDataBufBytes(maxDataSize int) error {
	ctx.sizeBuf = bytesutil.Resize(ctx.sizeBuf, 8)
	if _, err := io.ReadFull(ctx.bc, ctx.sizeBuf); err != nil {
		if err == io.EOF {
			return err
		}
		return fmt.Errorf("cannot read data size: %w", err)
	}
	dataSize := encoding.UnmarshalUint64(ctx.sizeBuf)
	if dataSize > uint64(maxDataSize) {
		return fmt.Errorf("too big data size: %d; it mustn't exceed %d bytes", dataSize, maxDataSize)
	}
	ctx.dataBuf = bytesutil.Resize(ctx.dataBuf, int(dataSize))
	if dataSize == 0 {
		return nil
	}
	if n, err := io.ReadFull(ctx.bc, ctx.dataBuf); err != nil {
		return fmt.Errorf("cannot read data with size %d: %w; read only %d bytes", dataSize, err, n)
	}
	return nil
}

func (ctx *vmselectRequestCtx) readByte() (byte, error) {
	ctx.dataBuf = bytesutil.Resize(ctx.dataBuf, 1)
	if _, err := io.ReadFull(ctx.bc, ctx.dataBuf); err != nil {
		if err == io.EOF {
			return 0, err
		}
		return 0, fmt.Errorf("cannot read byte: %w", err)
	}
	b := ctx.dataBuf[0]
	return b, nil
}

func (ctx *vmselectRequestCtx) writeDataBufBytes() error {
	if err := ctx.writeUint64(uint64(len(ctx.dataBuf))); err != nil {
		return fmt.Errorf("cannot write data size: %w", err)
	}
	if len(ctx.dataBuf) == 0 {
		return nil
	}
	if _, err := ctx.bc.Write(ctx.dataBuf); err != nil {
		return fmt.Errorf("cannot write data with size %d: %w", len(ctx.dataBuf), err)
	}
	return nil
}

// maxErrorMessageSize is the maximum size of error message to send to clients.
const maxErrorMessageSize = 64 * 1024

func (ctx *vmselectRequestCtx) writeErrorMessage(err error) error {
	if errors.Is(err, storage.ErrDeadlineExceeded) {
		err = fmt.Errorf("cannot execute request in %d seconds: %w", ctx.timeout, err)
	}
	errMsg := err.Error()
	if len(errMsg) > maxErrorMessageSize {
		// Trim too long error message.
		errMsg = errMsg[:maxErrorMessageSize]
	}
	if err := ctx.writeString(errMsg); err != nil {
		return fmt.Errorf("cannot send error message %q to client: %w", errMsg, err)
	}
	return nil
}

func (ctx *vmselectRequestCtx) writeString(s string) error {
	ctx.dataBuf = append(ctx.dataBuf[:0], s...)
	return ctx.writeDataBufBytes()
}

func (ctx *vmselectRequestCtx) writeUint64(n uint64) error {
	ctx.sizeBuf = encoding.MarshalUint64(ctx.sizeBuf[:0], n)
	if _, err := ctx.bc.Write(ctx.sizeBuf); err != nil {
		return fmt.Errorf("cannot write uint64 %d: %w", n, err)
	}
	return nil
}

const maxRPCNameSize = 128

var zeroTime time.Time

func (s *Server) processVMSelectRequest(ctx *vmselectRequestCtx) error {
	// Read rpcName
	// Do not set deadline on reading rpcName, since it may take a
	// lot of time for idle connection.
	if err := ctx.readDataBufBytes(maxRPCNameSize); err != nil {
		if err == io.EOF {
			// Remote client gracefully closed the connection.
			return err
		}
		return fmt.Errorf("cannot read rpcName: %w", err)
	}
	rpcName := string(ctx.dataBuf)

	// Limit the time required for reading request args.
	if err := ctx.bc.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return fmt.Errorf("cannot set read deadline for reading request args: %w", err)
	}
	defer func() {
		_ = ctx.bc.SetReadDeadline(zeroTime)
	}()

	// Read the timeout for request execution.
	timeout, err := ctx.readUint32()
	if err != nil {
		return fmt.Errorf("cannot read timeout for the request %q: %w", rpcName, err)
	}
	ctx.timeout = uint64(timeout)
	ctx.deadline = fasttime.UnixTimestamp() + uint64(timeout)

	switch rpcName {
	case "search_v5":
		return s.processVMSelectSearchQuery(ctx)
	case "labelValues_v2":
		return s.processVMSelectLabelValues(ctx)
	case "labelEntries_v2":
		return s.processVMSelectLabelEntries(ctx)
	case "labels_v2":
		return s.processVMSelectLabels(ctx)
	default:
		return fmt.Errorf("unsupported rpcName: %q", ctx.dataBuf)
	}
}

func (s *Server) processVMSelectLabels(ctx *vmselectRequestCtx) error {
	vmselectLabelsRequests.Inc()

	// Search for tag keys
	labels, err := s.storage.SearchTagKeys(*maxTagKeysPerSearch, ctx.deadline)
	if err != nil {
		return ctx.writeErrorMessage(err)
	}

	// Send an empty error message to vmselect.
	if err := ctx.writeString(""); err != nil {
		return fmt.Errorf("cannot send empty error message: %w", err)
	}

	// Send labels to vmselect
	for _, label := range labels {
		if len(label) == 0 {
			// Do this substitution in order to prevent clashing with 'end of response' marker.
			label = "__name__"
		}
		if err := ctx.writeString(label); err != nil {
			return fmt.Errorf("cannot write label %q: %w", label, err)
		}
	}

	// Send 'end of response' marker
	if err := ctx.writeString(""); err != nil {
		return fmt.Errorf("cannot send 'end of response' marker")
	}
	return nil
}

const maxLabelValueSize = 16 * 1024

func (s *Server) processVMSelectLabelValues(ctx *vmselectRequestCtx) error {
	vmselectLabelValuesRequests.Inc()

	// Read request
	if err := ctx.readDataBufBytes(maxLabelValueSize); err != nil {
		return fmt.Errorf("cannot read labelName: %w", err)
	}
	labelName := ctx.dataBuf

	// Search for tag values
	labelValues, err := s.storage.SearchTagValues(labelName, *maxTagValuesPerSearch, ctx.deadline)
	if err != nil {
		return ctx.writeErrorMessage(err)
	}

	// Send an empty error message to vmselect.
	if err := ctx.writeString(""); err != nil {
		return fmt.Errorf("cannot send empty error message: %w", err)
	}

	return writeLabelValues(ctx, labelValues)
}

func writeLabelValues(ctx *vmselectRequestCtx, labelValues []string) error {
	for _, labelValue := range labelValues {
		if len(labelValue) == 0 {
			// Skip empty label values, since they have no sense for prometheus.
			continue
		}
		if err := ctx.writeString(labelValue); err != nil {
			return fmt.Errorf("cannot write labelValue %q: %w", labelValue, err)
		}
	}
	// Send 'end of label values' marker
	if err := ctx.writeString(""); err != nil {
		return fmt.Errorf("cannot send 'end of response' marker")
	}
	return nil
}

func (s *Server) processVMSelectLabelEntries(ctx *vmselectRequestCtx) error {
	vmselectLabelEntriesRequests.Inc()

	// Perform the request
	labelEntries, err := s.storage.SearchTagEntries(*maxTagKeysPerSearch, *maxTagValuesPerSearch, ctx.deadline)
	if err != nil {
		return ctx.writeErrorMessage(err)
	}

	// Send an empty error message to vmselect.
	if err := ctx.writeString(""); err != nil {
		return fmt.Errorf("cannot send empty error message: %w", err)
	}

	// Send labelEntries to vmselect
	for i := range labelEntries {
		e := &labelEntries[i]
		label := e.Key
		if label == "" {
			// Do this substitution in order to prevent clashing with 'end of response' marker.
			label = "__name__"
		}
		if err := ctx.writeString(label); err != nil {
			return fmt.Errorf("cannot write label %q: %w", label, err)
		}
		if err := writeLabelValues(ctx, e.Values); err != nil {
			return fmt.Errorf("cannot write label values for %q: %w", label, err)
		}
	}

	// Send 'end of response' marker
	if err := ctx.writeString(""); err != nil {
		return fmt.Errorf("cannot send 'end of response' marker")
	}
	return nil
}

// maxSearchQuerySize is the maximum size of SearchQuery packet in bytes.
const maxSearchQuerySize = 1024 * 1024

func (s *Server) processVMSelectSearchQuery(ctx *vmselectRequestCtx) error {
	vmselectSearchQueryRequests.Inc()

	// Read search query.
	if err := ctx.readDataBufBytes(maxSearchQuerySize); err != nil {
		return fmt.Errorf("cannot read searchQuery: %w", err)
	}
	tail, err := ctx.sq.Unmarshal(ctx.dataBuf)
	if err != nil {
		return fmt.Errorf("cannot unmarshal SearchQuery: %w", err)
	}
	if len(tail) > 0 {
		return fmt.Errorf("unexpected non-zero tail left after unmarshaling SearchQuery: (len=%d) %q", len(tail), tail)
	}

	// Setup search.
	if err := ctx.setupTfss(); err != nil {
		return ctx.writeErrorMessage(err)
	}
	tr := storage.ScanRange{
		MinTimestamp: ctx.sq.MinTimestamp,
		MaxTimestamp: ctx.sq.MaxTimestamp,
		Limit:        ctx.sq.Limit,
	}
	if err := checkTimeRange(s.storage, tr); err != nil {
		return ctx.writeErrorMessage(err)
	}
	ctx.sr.Init(s.storage, ctx.tfss, tr, ctx.deadline)
	defer ctx.sr.MustClose()
	if err := ctx.sr.Error(); err != nil {
		return ctx.writeErrorMessage(err)
	}

	// Send empty error message to vmselect.
	if err := ctx.writeString(""); err != nil {
		return fmt.Errorf("cannot send empty error message: %w", err)
	}

	// Send found blocks to vmselect.
	for ctx.sr.NextMetricBlock() {
		ctx.sr.BlockRef.MustReadBlock(&ctx.mb, ctx.sq.FetchData)

		vmselectMetricBlocksRead.Inc()
		vmselectMetricRowsRead.Add(ctx.mb.RowsCount())

		ctx.dataBuf = storage.MarshalBlock(ctx.dataBuf[:0], &ctx.mb)
		if err := ctx.writeDataBufBytes(); err != nil {
			return fmt.Errorf("cannot send MetricBlock: %w", err)
		}
	}
	if err := ctx.sr.Error(); err != nil {
		return fmt.Errorf("search error: %w", err)
	}

	// Send 'end of response' marker
	if err := ctx.writeString(""); err != nil {
		return fmt.Errorf("cannot send 'end of response' marker")
	}
	return nil
}

// checkTimeRange returns true if the given tr is denied for querying.
func checkTimeRange(s *storage.Storage, tr storage.ScanRange) error {
	if !*denyQueriesOutsideRetention {
		return nil
	}
	retentionPeriod := s.RetentionMonths()
	minAllowedTimestamp := (int64(fasttime.UnixTimestamp()) - int64(retentionPeriod)*3600*24*30) * 1000
	if tr.MinTimestamp > minAllowedTimestamp {
		return nil
	}
	return &httpserver.ErrorWithStatusCode{
		Err:        fmt.Errorf("the given time range %s is outside the allowed retention of %d months according to -denyQueriesOutsideRetention", &tr, retentionPeriod),
		StatusCode: http.StatusServiceUnavailable,
	}
}

var (
	vmselectLabelsRequests       = metrics.NewCounter("vm_vmselect_labels_requests_total")
	vmselectLabelValuesRequests  = metrics.NewCounter("vm_vmselect_label_values_requests_total")
	vmselectLabelEntriesRequests = metrics.NewCounter("vm_vmselect_label_entries_requests_total")
	vmselectSearchQueryRequests  = metrics.NewCounter("vm_vmselect_search_query_requests_total")
	vmselectMetricBlocksRead     = metrics.NewCounter("vm_vmselect_metric_blocks_read_total")
	vmselectMetricRowsRead       = metrics.NewCounter("vm_vmselect_metric_rows_read_total")
)

func (ctx *vmselectRequestCtx) setupTfss() error {
	tfss := ctx.tfss[:0]
	for _, tagFilters := range ctx.sq.TagFilterss {
		tfs := storage.NewTagFilters()
		for i := range tagFilters {
			tf := &tagFilters[i]
			if err := tfs.Add(tf.Key, tf.Value, tf.IsNegative, tf.IsRegexp); err != nil {
				return fmt.Errorf("cannot parse tag filter %s: %w", tf, err)
			}
		}
		tfss = append(tfss, tfs)
		tfss = append(tfss, tfs.Finalize()...)
	}
	ctx.tfss = tfss
	return nil
}
