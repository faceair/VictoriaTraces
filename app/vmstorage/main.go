package main

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/buildinfo"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/envflag"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"
	"github.com/VictoriaMetrics/metrics"
	"github.com/faceair/VictoriaTraces/app/vmstorage/transport/vmstorage"
	"github.com/faceair/VictoriaTraces/lib/storage"
)

var (
	retentionPeriod = flagutil.NewDuration("retentionPeriod", 1, "Data with timestamps outside the retentionPeriod is automatically deleted")
	storageDataPath = flag.String("storageDataPath", "vmstorage-data", "Path to storage data")
	vminsertAddr    = flag.String("vminsertAddr", ":8400", "TCP address to accept connections from vminsert services")
	vmselectAddr    = flag.String("vmselectAddr", ":8401", "TCP address to accept connections from vmselect services")
	finalMergeDelay = flag.Duration("finalMergeDelay", 30*time.Second, "The delay before starting final merge for per-month partition after no new data is ingested into it. "+
		"Query speed and disk space usage is usually reduced after the final merge is complete. Too low delay for final merge may result in increased "+
		"disk IO usage and CPU usage")
	bigMergeConcurrency   = flag.Int("bigMergeConcurrency", 0, "The maximum number of CPU cores to use for big merges. Default value is used if set to 0")
	smallMergeConcurrency = flag.Int("smallMergeConcurrency", 0, "The maximum number of CPU cores to use for small merges. Default value is used if set to 0")
)

func main() {
	// Write flags and help message to stdout, since it is easier to grep or pipe.
	flag.CommandLine.SetOutput(os.Stdout)
	envflag.Parse()
	buildinfo.Init()
	logger.Init()

	storage.SetFinalMergeDelay(*finalMergeDelay)
	storage.SetBigMergeWorkersCount(*bigMergeConcurrency)
	storage.SetSmallMergeWorkersCount(*smallMergeConcurrency)

	logger.Infof("opening storage at %q with -retentionPeriod=%s", *storageDataPath, retentionPeriod)
	startTime := time.Now()
	strg, err := storage.OpenStorage(*storageDataPath, retentionPeriod.Msecs)
	if err != nil {
		logger.Fatalf("cannot open a storage at %s with -retentionPeriod=%s: %s", *storageDataPath, retentionPeriod, err)
	}

	var m storage.Metrics
	strg.UpdateMetrics(&m)
	tm := &m.TableMetrics
	partsCount := tm.SmallPartsCount + tm.BigPartsCount
	blocksCount := tm.SmallBlocksCount + tm.BigBlocksCount
	rowsCount := tm.SmallRowsCount + tm.BigRowsCount
	sizeBytes := tm.SmallSizeBytes + tm.BigSizeBytes
	logger.Infof("successfully opened storage %q in %.3f seconds; partsCount: %d; blocksCount: %d; rowsCount: %d; sizeBytes: %d",
		*storageDataPath, time.Since(startTime).Seconds(), partsCount, blocksCount, rowsCount, sizeBytes)

	registerStorageMetrics(strg)

	vmstorage.StartUnmarshalWorkers()
	srv, err := vmstorage.NewServer(*vminsertAddr, *vmselectAddr, strg)
	if err != nil {
		logger.Fatalf("cannot create a server with vminsertAddr=%s, vmselectAddr=%s: %s", *vminsertAddr, *vmselectAddr, err)
	}

	go srv.RunVMInsert()
	go srv.RunVMSelect()

	sig := procutil.WaitForSigterm()
	logger.Infof("service received signal %s", sig)

	logger.Infof("gracefully shutting down the service")
	startTime = time.Now()
	srv.MustClose()
	vmstorage.StopUnmarshalWorkers()
	logger.Infof("successfully shut down the service in %.3f seconds", time.Since(startTime).Seconds())

	logger.Infof("gracefully closing the storage at %s", *storageDataPath)
	startTime = time.Now()
	strg.MustClose()
	logger.Infof("successfully closed the storage in %.3f seconds", time.Since(startTime).Seconds())

	fs.MustStopDirRemover()

	logger.Infof("the vmstorage has been stopped")
}

func registerStorageMetrics(strg *storage.Storage) {
	mCache := &storage.Metrics{}
	var mCacheLock sync.Mutex
	var lastUpdateTime time.Time

	m := func() *storage.Metrics {
		mCacheLock.Lock()
		defer mCacheLock.Unlock()
		if time.Since(lastUpdateTime) < time.Second {
			return mCache
		}
		var mc storage.Metrics
		strg.UpdateMetrics(&mc)
		mCache = &mc
		lastUpdateTime = time.Now()
		return mCache
	}
	tm := func() *storage.TableMetrics {
		sm := m()
		return &sm.TableMetrics
	}
	idbm := func() *storage.IndexDBMetrics {
		sm := m()
		return &sm.IndexDBMetrics
	}

	metrics.NewGauge(fmt.Sprintf(`vm_free_disk_space_bytes{path=%q}`, *storageDataPath), func() float64 {
		return float64(fs.MustGetFreeSpace(*storageDataPath))
	})

	metrics.NewGauge(`vm_active_merges{type="storage/big"}`, func() float64 {
		return float64(tm().ActiveBigMerges)
	})
	metrics.NewGauge(`vm_active_merges{type="storage/small"}`, func() float64 {
		return float64(tm().ActiveSmallMerges)
	})
	metrics.NewGauge(`vm_active_merges{type="indexdb"}`, func() float64 {
		return float64(idbm().ActiveMerges)
	})

	metrics.NewGauge(`vm_merges_total{type="storage/big"}`, func() float64 {
		return float64(tm().BigMergesCount)
	})
	metrics.NewGauge(`vm_merges_total{type="storage/small"}`, func() float64 {
		return float64(tm().SmallMergesCount)
	})
	metrics.NewGauge(`vm_merges_total{type="indexdb"}`, func() float64 {
		return float64(idbm().MergesCount)
	})

	metrics.NewGauge(`vm_rows_merged_total{type="storage/big"}`, func() float64 {
		return float64(tm().BigRowsMerged)
	})
	metrics.NewGauge(`vm_rows_merged_total{type="storage/small"}`, func() float64 {
		return float64(tm().SmallRowsMerged)
	})
	metrics.NewGauge(`vm_rows_merged_total{type="indexdb"}`, func() float64 {
		return float64(idbm().ItemsMerged)
	})

	metrics.NewGauge(`vm_rows_deleted_total{type="storage/big"}`, func() float64 {
		return float64(tm().BigRowsDeleted)
	})
	metrics.NewGauge(`vm_rows_deleted_total{type="storage/small"}`, func() float64 {
		return float64(tm().SmallRowsDeleted)
	})

	metrics.NewGauge(`vm_references{type="storage/big", name="parts"}`, func() float64 {
		return float64(tm().BigPartsRefCount)
	})
	metrics.NewGauge(`vm_references{type="storage/small", name="parts"}`, func() float64 {
		return float64(tm().SmallPartsRefCount)
	})
	metrics.NewGauge(`vm_references{type="storage", name="partitions"}`, func() float64 {
		return float64(tm().PartitionsRefCount)
	})
	metrics.NewGauge(`vm_references{type="indexdb", name="objects"}`, func() float64 {
		return float64(idbm().IndexDBRefCount)
	})
	metrics.NewGauge(`vm_references{type="indexdb", name="parts"}`, func() float64 {
		return float64(idbm().PartsRefCount)
	})

	metrics.NewGauge(`vm_new_timeseries_created_total`, func() float64 {
		return float64(idbm().NewTimeseriesCreated)
	})
	metrics.NewGauge(`vm_missing_tsids_for_metric_id_total`, func() float64 {
		return float64(idbm().MissingTSIDsForMetricID)
	})
	metrics.NewGauge(`vm_date_metric_ids_search_calls_total`, func() float64 {
		return float64(idbm().DateMetricIDsSearchCalls)
	})
	metrics.NewGauge(`vm_date_metric_ids_search_hits_total`, func() float64 {
		return float64(idbm().DateMetricIDsSearchHits)
	})
	metrics.NewGauge(`vm_index_blocks_with_metric_ids_processed_total`, func() float64 {
		return float64(idbm().IndexBlocksWithMetricIDsProcessed)
	})
	metrics.NewGauge(`vm_index_blocks_with_metric_ids_incorrect_order_total`, func() float64 {
		return float64(idbm().IndexBlocksWithMetricIDsIncorrectOrder)
	})

	metrics.NewGauge(`vm_assisted_merges_total{type="storage/small"}`, func() float64 {
		return float64(tm().SmallAssistedMerges)
	})
	metrics.NewGauge(`vm_assisted_merges_total{type="indexdb"}`, func() float64 {
		return float64(idbm().AssistedMerges)
	})

	// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/686
	metrics.NewGauge(`vm_merge_need_free_disk_space{type="storage/small"}`, func() float64 {
		return float64(tm().SmallMergeNeedFreeDiskSpace)
	})
	metrics.NewGauge(`vm_merge_need_free_disk_space{type="storage/big"}`, func() float64 {
		return float64(tm().BigMergeNeedFreeDiskSpace)
	})

	metrics.NewGauge(`vm_pending_rows{type="storage"}`, func() float64 {
		return float64(tm().PendingRows)
	})
	metrics.NewGauge(`vm_pending_rows{type="indexdb"}`, func() float64 {
		return float64(idbm().PendingItems)
	})

	metrics.NewGauge(`vm_parts{type="storage/big"}`, func() float64 {
		return float64(tm().BigPartsCount)
	})
	metrics.NewGauge(`vm_parts{type="storage/small"}`, func() float64 {
		return float64(tm().SmallPartsCount)
	})
	metrics.NewGauge(`vm_parts{type="indexdb"}`, func() float64 {
		return float64(idbm().PartsCount)
	})

	metrics.NewGauge(`vm_blocks{type="storage/big"}`, func() float64 {
		return float64(tm().BigBlocksCount)
	})
	metrics.NewGauge(`vm_blocks{type="storage/small"}`, func() float64 {
		return float64(tm().SmallBlocksCount)
	})
	metrics.NewGauge(`vm_blocks{type="indexdb"}`, func() float64 {
		return float64(idbm().BlocksCount)
	})

	metrics.NewGauge(`vm_data_size_bytes{type="storage/big"}`, func() float64 {
		return float64(tm().BigSizeBytes)
	})
	metrics.NewGauge(`vm_data_size_bytes{type="storage/small"}`, func() float64 {
		return float64(tm().SmallSizeBytes)
	})
	metrics.NewGauge(`vm_data_size_bytes{type="indexdb"}`, func() float64 {
		return float64(idbm().SizeBytes)
	})

	metrics.NewGauge(`vm_rows_added_to_storage_total`, func() float64 {
		return float64(m().RowsAddedTotal)
	})

	metrics.NewGauge(`vm_rows_ignored_total{reason="big_timestamp"}`, func() float64 {
		return float64(m().TooBigTimestampRows)
	})
	metrics.NewGauge(`vm_rows_ignored_total{reason="small_timestamp"}`, func() float64 {
		return float64(m().TooSmallTimestampRows)
	})

	metrics.NewGauge(`vm_concurrent_addrows_limit_reached_total`, func() float64 {
		return float64(m().AddRowsConcurrencyLimitReached)
	})
	metrics.NewGauge(`vm_concurrent_addrows_limit_timeout_total`, func() float64 {
		return float64(m().AddRowsConcurrencyLimitTimeout)
	})
	metrics.NewGauge(`vm_concurrent_addrows_dropped_rows_total`, func() float64 {
		return float64(m().AddRowsConcurrencyDroppedRows)
	})
	metrics.NewGauge(`vm_concurrent_addrows_capacity`, func() float64 {
		return float64(m().AddRowsConcurrencyCapacity)
	})
	metrics.NewGauge(`vm_concurrent_addrows_current`, func() float64 {
		return float64(m().AddRowsConcurrencyCurrent)
	})

	metrics.NewGauge(`vm_concurrent_search_tsids_limit_reached_total`, func() float64 {
		return float64(m().SearchTraceIDsConcurrencyLimitReached)
	})
	metrics.NewGauge(`vm_concurrent_search_tsids_limit_timeout_total`, func() float64 {
		return float64(m().SearchTraceIDsConcurrencyLimitTimeout)
	})
	metrics.NewGauge(`vm_concurrent_search_tsids_capacity`, func() float64 {
		return float64(m().SearchTraceIDsConcurrencyCapacity)
	})
	metrics.NewGauge(`vm_concurrent_search_tsids_current`, func() float64 {
		return float64(m().SearchTraceIDsConcurrencyCurrent)
	})

	metrics.NewGauge(`vm_search_delays_total`, func() float64 {
		return float64(m().SearchDelays)
	})

	metrics.NewGauge(`vm_slow_row_inserts_total`, func() float64 {
		return float64(m().SlowRowInserts)
	})
	metrics.NewGauge(`vm_slow_per_day_index_inserts_total`, func() float64 {
		return float64(m().SlowPerDayIndexInserts)
	})
	metrics.NewGauge(`vm_slow_metric_name_loads_total`, func() float64 {
		return float64(m().SlowMetricNameLoads)
	})

	metrics.NewGauge(`vm_timestamps_blocks_merged_total`, func() float64 {
		return float64(m().TimestampsBlocksMerged)
	})
	metrics.NewGauge(`vm_timestamps_bytes_saved_total`, func() float64 {
		return float64(m().TimestampsBytesSaved)
	})

	metrics.NewGauge(`vm_rows{type="storage/big"}`, func() float64 {
		return float64(tm().BigRowsCount)
	})
	metrics.NewGauge(`vm_rows{type="storage/small"}`, func() float64 {
		return float64(tm().SmallRowsCount)
	})
	metrics.NewGauge(`vm_rows{type="indexdb"}`, func() float64 {
		return float64(idbm().ItemsCount)
	})

	metrics.NewGauge(`vm_date_range_search_calls_total`, func() float64 {
		return float64(idbm().DateRangeSearchCalls)
	})
	metrics.NewGauge(`vm_date_range_hits_total`, func() float64 {
		return float64(idbm().DateRangeSearchHits)
	})

	metrics.NewGauge(`vm_missing_metric_names_for_metric_id_total`, func() float64 {
		return float64(idbm().MissingMetricNamesForMetricID)
	})

	metrics.NewGauge(`vm_cache_entries{type="storage/metricName"}`, func() float64 {
		return float64(m().MetricNameCacheSize)
	})
	metrics.NewGauge(`vm_cache_entries{type="storage/minute_metric_ids"}`, func() float64 {
		return float64(m().MinuteMetricIDCacheSize)
	})
	metrics.NewGauge(`vm_cache_entries{type="storage/bigIndexBlocks"}`, func() float64 {
		return float64(tm().BigIndexBlocksCacheSize)
	})
	metrics.NewGauge(`vm_cache_entries{type="storage/smallIndexBlocks"}`, func() float64 {
		return float64(tm().SmallIndexBlocksCacheSize)
	})
	metrics.NewGauge(`vm_cache_entries{type="indexdb/dataBlocks"}`, func() float64 {
		return float64(idbm().DataBlocksCacheSize)
	})
	metrics.NewGauge(`vm_cache_entries{type="indexdb/indexBlocks"}`, func() float64 {
		return float64(idbm().IndexBlocksCacheSize)
	})
	metrics.NewGauge(`vm_cache_entries{type="indexdb/tagFilters"}`, func() float64 {
		return float64(idbm().TagCacheSize)
	})
	metrics.NewGauge(`vm_cache_entries{type="indexdb/uselessTagFilters"}`, func() float64 {
		return float64(idbm().UselessTagFiltersCacheSize)
	})
	metrics.NewGauge(`vm_cache_entries{type="storage/regexps"}`, func() float64 {
		return float64(storage.RegexpCacheSize())
	})

	metrics.NewGauge(`vm_cache_size_bytes{type="storage/metricName"}`, func() float64 {
		return float64(m().MetricNameCacheSizeBytes)
	})
	metrics.NewGauge(`vm_cache_size_bytes{type="storage/minute_metric_ids"}`, func() float64 {
		return float64(m().MinuteMetricIDCacheSizeBytes)
	})
	metrics.NewGauge(`vm_cache_size_bytes{type="indexdb/tagFilters"}`, func() float64 {
		return float64(idbm().TagCacheSizeBytes)
	})
	metrics.NewGauge(`vm_cache_size_bytes{type="indexdb/uselessTagFilters"}`, func() float64 {
		return float64(idbm().UselessTagFiltersCacheSizeBytes)
	})
	metrics.NewGauge(`vm_cache_requests_total{type="storage/metricName"}`, func() float64 {
		return float64(m().MetricNameCacheRequests)
	})
	metrics.NewGauge(`vm_cache_requests_total{type="storage/bigIndexBlocks"}`, func() float64 {
		return float64(tm().BigIndexBlocksCacheRequests)
	})
	metrics.NewGauge(`vm_cache_requests_total{type="storage/smallIndexBlocks"}`, func() float64 {
		return float64(tm().SmallIndexBlocksCacheRequests)
	})
	metrics.NewGauge(`vm_cache_requests_total{type="indexdb/dataBlocks"}`, func() float64 {
		return float64(idbm().DataBlocksCacheRequests)
	})
	metrics.NewGauge(`vm_cache_requests_total{type="indexdb/indexBlocks"}`, func() float64 {
		return float64(idbm().IndexBlocksCacheRequests)
	})
	metrics.NewGauge(`vm_cache_requests_total{type="indexdb/tagFilters"}`, func() float64 {
		return float64(idbm().TagCacheRequests)
	})
	metrics.NewGauge(`vm_cache_requests_total{type="indexdb/uselessTagFilters"}`, func() float64 {
		return float64(idbm().UselessTagFiltersCacheRequests)
	})
	metrics.NewGauge(`vm_cache_requests_total{type="storage/regexps"}`, func() float64 {
		return float64(storage.RegexpCacheRequests())
	})

	metrics.NewGauge(`vm_cache_misses_total{type="storage/metricName"}`, func() float64 {
		return float64(m().MetricNameCacheMisses)
	})
	metrics.NewGauge(`vm_cache_misses_total{type="storage/bigIndexBlocks"}`, func() float64 {
		return float64(tm().BigIndexBlocksCacheMisses)
	})
	metrics.NewGauge(`vm_cache_misses_total{type="storage/smallIndexBlocks"}`, func() float64 {
		return float64(tm().SmallIndexBlocksCacheMisses)
	})
	metrics.NewGauge(`vm_cache_misses_total{type="indexdb/dataBlocks"}`, func() float64 {
		return float64(idbm().DataBlocksCacheMisses)
	})
	metrics.NewGauge(`vm_cache_misses_total{type="indexdb/indexBlocks"}`, func() float64 {
		return float64(idbm().IndexBlocksCacheMisses)
	})
	metrics.NewGauge(`vm_cache_misses_total{type="indexdb/tagFilters"}`, func() float64 {
		return float64(idbm().TagCacheMisses)
	})
	metrics.NewGauge(`vm_cache_misses_total{type="indexdb/uselessTagFilters"}`, func() float64 {
		return float64(idbm().UselessTagFiltersCacheMisses)
	})
	metrics.NewGauge(`vm_cache_misses_total{type="storage/regexps"}`, func() float64 {
		return float64(storage.RegexpCacheMisses())
	})

	metrics.NewGauge(`vm_cache_collisions_total{type="storage/metricName"}`, func() float64 {
		return float64(m().MetricNameCacheCollisions)
	})
}
