package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/searchutils"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/metrics"
	"github.com/faceair/VictoriaTraces/app/vmstorage/transport/vminsert"
	"github.com/faceair/VictoriaTraces/app/vmstorage/transport/vmselect"
	"github.com/faceair/VictoriaTraces/lib/storage"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

var isInTest = func() bool {
	return strings.HasSuffix(os.Args[0], ".test")
}()

var rowsInserted = metrics.NewCounter(`vm_rows_inserted_total{type="span"}`)

func NewStore() *Store {
	return new(Store)
}

type Store struct{}

func (s *Store) DependencyReader() dependencystore.Reader {
	return s
}

func (s *Store) SpanReader() spanstore.Reader {
	return s
}

func (s *Store) SpanWriter() spanstore.Writer {
	return s
}

func (s *Store) GetTrace(_ context.Context, traceID model.TraceID) (*model.Trace, error) {
	deadline := searchutils.NewDeadline(time.Now(), time.Minute, "")

	results, _, err := vmselect.ProcessSearchQuery(false, &storage.SearchQuery{
		MinTimestamp: timestampFromTime(time.Now().Add(-time.Hour)),
		MaxTimestamp: timestampFromTime(time.Now()),
		TagFilterss: [][]storage.TagFilter{
			{
				{
					Key:   nil,
					Value: bytesutil.ToUnsafeBytes(traceID.String()),
				},
			},
		},
		Limit:     1,
		Forward:   true,
		FetchData: storage.FetchAll,
	}, deadline)
	if err != nil {
		return nil, err
	}

	traces := make([]*model.Trace, 0)
	err = results.RunParallel(func(rs *vmselect.Result, workerID uint) error {
		trace := &model.Trace{
			Spans:      make([]*model.Span, 0),
			ProcessMap: nil,
			Warnings:   nil,
		}
		for _, value := range rs.Values {
			span := new(model.Span)
			err = json.Unmarshal(value, span)
			if err != nil {
				return err
			}
			trace.Spans = append(trace.Spans, span)
		}
		traces = append(traces, trace)
		return nil
	})
	if len(traces) == 0 {
		return nil, spanstore.ErrTraceNotFound
	}
	return traces[0], err
}

func (s *Store) GetServices(_ context.Context) ([]string, error) {
	deadline := searchutils.NewDeadline(time.Now(), time.Minute, "")

	services, _, err := vmselect.GetLabelValues(false, "service", deadline)
	if err != nil {
		return nil, fmt.Errorf(`cannot obtain label values for service: %w`, err)
	}
	return services, err
}

func (s *Store) GetOperations(_ context.Context, query spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	deadline := searchutils.NewDeadline(time.Now(), time.Minute, "")

	results, _, err := vmselect.ProcessSearchQuery(false, &storage.SearchQuery{
		MinTimestamp: timestampFromTime(time.Now().Add(-time.Hour)),
		MaxTimestamp: timestampFromTime(time.Now()),
		TagFilterss: [][]storage.TagFilter{
			{
				{
					Key:   []byte("service"),
					Value: []byte(query.ServiceName),
				},
			},
		},
		Limit:     1e4,
		Forward:   true,
		FetchData: storage.FetchAll,
	}, deadline)
	if err != nil {
		return nil, err
	}

	var rsLock sync.Mutex
	operationsMap := make(map[string]map[string]struct{})
	err = results.RunParallel(func(rs *vmselect.Result, workerID uint) error {
		for _, value := range rs.Values {
			span := new(model.Span)
			err = json.Unmarshal(value, span)
			if err != nil {
				return err
			}
			kind, _ := span.GetSpanKind()
			rsLock.Lock()
			kinds, ok := operationsMap[span.OperationName]
			if !ok {
				kinds = map[string]struct{}{}
			}
			kinds[kind] = struct{}{}
			operationsMap[span.OperationName] = kinds
			rsLock.Unlock()
		}
		return nil
	})
	operations := make([]spanstore.Operation, 0)
	for operation, kinds := range operationsMap {
		for kind := range kinds {
			operations = append(operations, spanstore.Operation{
				Name:     operation,
				SpanKind: kind,
			})
		}
	}
	return operations, nil
}

func (s *Store) FindTraces(_ context.Context, query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	deadline := searchutils.NewDeadline(time.Now(), time.Minute, "")

	searchQuery := traceQueryToSearchQuery(query)
	searchQuery.FetchData = storage.FetchAll

	results, _, err := vmselect.ProcessSearchQuery(false, searchQuery, deadline)
	if err != nil {
		return nil, err
	}

	var rsLock sync.Mutex
	traces := make([]*model.Trace, 0)
	err = results.RunParallel(func(rs *vmselect.Result, workerID uint) error {
		trace := &model.Trace{
			Spans:      make([]*model.Span, 0),
			ProcessMap: nil,
			Warnings:   nil,
		}
		for _, value := range rs.Values {
			span := new(model.Span)
			err = json.Unmarshal(value, span)
			if err != nil {
				return err
			}
			trace.Spans = append(trace.Spans, span)
		}
		rsLock.Lock()
		traces = append(traces, trace)
		rsLock.Unlock()
		return nil
	})
	return traces, err
}

func (s *Store) FindTraceIDs(_ context.Context, query *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	deadline := searchutils.NewDeadline(time.Now(), time.Minute, "")

	results, _, err := vmselect.SearchTraceIDs(false, traceQueryToSearchQuery(query), deadline)
	if err != nil {
		return nil, err
	}

	traceIDs := make([]model.TraceID, 0)
	for _, result := range results {
		traceIDs = append(traceIDs, model.TraceID{Low: result.Lo, High: result.Hi})
	}
	return traceIDs, err
}

func (s *Store) WriteSpan(_ context.Context, span *model.Span) error {
	ctx := vminsert.GetInsertCtx()
	defer vminsert.PutInsertCtx(ctx)
	ctx.Reset()

	ctx.Labels = ctx.Labels[:0]

	ctx.AddLabel(nil, bytesutil.ToUnsafeBytes(span.TraceID.String()))
	ctx.AddLabel([]byte("service"), []byte(span.Process.ServiceName))
	ctx.AddLabel([]byte("operation_name"), []byte(span.OperationName))

	body, err := json.Marshal(span)
	if err != nil {
		return err
	}

	err = ctx.WriteDataPoint(ctx.Labels, span.StartTime.UnixNano()/int64(time.Millisecond), body)
	if err != nil {
		return err
	}

	if isInTest {
		if err = ctx.FlushBufs(); err != nil {
			return err
		}
	}

	rowsInserted.Add(1)
	return nil
}

func (s *Store) GetDependencies(ctx context.Context, endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	return nil, nil
}

func (s *Store) WriteDependencies(ts time.Time, dependencies []model.DependencyLink) error {
	return nil
}

// timestampFromTime returns timestamp value for the given time.
func timestampFromTime(t time.Time) int64 {
	// There is no need in converting t to UTC, since UnixNano must
	// return the same value for any timezone.
	return t.UnixNano() / 1e6
}

func traceQueryToSearchQuery(query *spanstore.TraceQueryParameters) *storage.SearchQuery {
	if query.StartTimeMin.IsZero() {
		query.StartTimeMin = time.Now().Add(-time.Hour)
	}
	if query.StartTimeMax.IsZero() {
		query.StartTimeMax = time.Now()
	}

	ts := []storage.TagFilter{{
		Key:   []byte("service"),
		Value: []byte(query.ServiceName),
	}}
	if query.OperationName != "" {
		ts = append(ts, storage.TagFilter{
			Key:   []byte("operation_name"),
			Value: []byte(query.OperationName),
		})
	}
	return &storage.SearchQuery{
		MinTimestamp: timestampFromTime(query.StartTimeMin),
		MaxTimestamp: timestampFromTime(query.StartTimeMax),
		TagFilterss:  [][]storage.TagFilter{ts},
		Limit:        query.NumTraces,
		Forward:      true,
		FetchData:    storage.NotFetch,
	}
}
