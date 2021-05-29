package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/buildinfo"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/envflag"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/common"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/writeconcurrencylimiter"
	"github.com/VictoriaMetrics/metrics"
	"github.com/faceair/VictoriaTraces/app/jaeger-agent/store"
	"github.com/faceair/VictoriaTraces/app/vmstorage/transport/vminsert"
	"github.com/faceair/VictoriaTraces/app/vmstorage/transport/vmselect"
	"github.com/faceair/VictoriaTraces/lib/storage"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
)

var (
	maxLabelsPerTimeseries = flag.Int("maxLabelsPerTimeseries", 30, "The maximum number of labels accepted per time series. Superflouos labels are dropped")
	httpListenAddr         = flag.String("httpListenAddr", ":8480", "Address to listen for http connections")
	cacheDataPath          = flag.String("cacheDataPath", "", "Path to directory for cache files. Cache isn't saved if empty")
	vminsertNodes          = flagutil.NewArray("vminsertNodes", "Address of vminsert nodes; usage: -storageNode=vmstorage-host1:8400 -storageNode=vmstorage-host2:8400")
	vmselectNodes          = flagutil.NewArray("vmselectNodes", "Address of vmselect nodes; usage: -storageNode=vmstorage-host1:8400 -storageNode=vmstorage-host2:8400")
)

func main() {
	// Write flags and help message to stdout, since it is easier to grep or pipe.
	flag.CommandLine.SetOutput(os.Stdout)
	envflag.Parse()
	buildinfo.Init()
	logger.Init()

	logger.Infof("initializing netstorage for storageNodes %s and %s...", *vminsertNodes, *vmselectNodes)
	startTime := time.Now()
	if len(*vminsertNodes) == 0 || len(*vmselectNodes) == 0 {
		logger.Fatalf("missing -vminsertNodes or -vmselectNodes arg")
	}
	vminsert.InitStorageNodes(*vminsertNodes)
	vmselect.InitStorageNodes(*vmselectNodes)
	logger.Infof("successfully initialized netstorage in %.3f seconds", time.Since(startTime).Seconds())

	if len(*cacheDataPath) > 0 {
		tmpDataPath := *cacheDataPath + "/tmp"
		fs.RemoveDirContents(tmpDataPath)
		vmselect.InitTmpBlocksDir(tmpDataPath)
	} else {
		vmselect.InitTmpBlocksDir("")
	}

	storage.SetMaxLabelsPerTimeseries(*maxLabelsPerTimeseries)
	common.StartUnmarshalWorkers()
	writeconcurrencylimiter.Init()

	grpc.Serve(&shared.PluginServices{Store: store.NewStore()})

	go func() {
		httpserver.Serve(*httpListenAddr, requestHandler)
	}()

	sig := procutil.WaitForSigterm()
	logger.Infof("service received signal %s", sig)

	startTime = time.Now()
	logger.Infof("successfully shut down http service in %.3f seconds", time.Since(startTime).Seconds())

	common.StopUnmarshalWorkers()

	logger.Infof("shutting down neststorage...")
	startTime = time.Now()
	vminsert.Stop()
	vmselect.Stop()
	logger.Infof("successfully stopped netstorage in %.3f seconds", time.Since(startTime).Seconds())

	fs.MustStopDirRemover()

	logger.Infof("the jaeger-agent has been stopped")
}

func requestHandler(w http.ResponseWriter, r *http.Request) bool {
	if r.URL.Path == "/" {
		if r.Method != "GET" {
			return false
		}
		fmt.Fprintf(w, "jaeger-agent - a component of VictoriaTraces. See docs at https://github.com/faceair/VictoriaTraces")
		return true
	}
	return false
}

var (
	_ = metrics.NewGauge(`vm_metrics_with_dropped_labels_total`, func() float64 {
		return float64(atomic.LoadUint64(&storage.MetricsWithDroppedLabels))
	})
	_ = metrics.NewGauge(`vm_too_long_label_names_total`, func() float64 {
		return float64(atomic.LoadUint64(&storage.TooLongLabelNames))
	})
	_ = metrics.NewGauge(`vm_too_long_label_values_total`, func() float64 {
		return float64(atomic.LoadUint64(&storage.TooLongLabelValues))
	})
)
