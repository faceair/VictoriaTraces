package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/searchutils"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/metrics"
	"github.com/faceair/VictoriaTraces/app/vmstorage/transport/vminsert"
	"github.com/faceair/VictoriaTraces/app/vmstorage/transport/vmselect"
	"github.com/faceair/VictoriaTraces/lib/storage"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

var rowsInserted = metrics.NewCounter(`vm_rows_inserted_total{type="span"}`)

type Store struct{}

func (s *Store) GetTrace(_ context.Context, traceID model.TraceID) (*model.Trace, error) {
	deadline := searchutils.NewDeadline(time.Now(), time.Minute, "")

	results, _, err := vmselect.ProcessSearchQuery(false, &storage.SearchQuery{
		MinTimestamp: timestampFromTime(time.Now().Add(-time.Hour * 24 * 7)),
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

	spanNames, _, err := vmselect.SearchMetricNames(false, &storage.SearchQuery{
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
		FetchData: storage.NotFetch,
	}, deadline)
	if err != nil {
		return nil, err
	}
	operations := make([]spanstore.Operation, 0)
	for _, spanName := range spanNames {
		for _, tag := range spanName.Tags {
			if string(tag.Key) == "operation_name" {
				operations = append(operations, spanstore.Operation{
					Name: string(tag.Value),
				})
			}
		}
	}
	return operations, nil
}

func (s *Store) FindTraces(_ context.Context, query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	deadline := searchutils.NewDeadline(time.Now(), time.Minute, "")

	results, _, err := vmselect.ProcessSearchQuery(false, &storage.SearchQuery{
		MinTimestamp: timestampFromTime(query.StartTimeMin),
		MaxTimestamp: timestampFromTime(query.StartTimeMax),
		TagFilterss: [][]storage.TagFilter{
			{
				{
					Key:   []byte("service"),
					Value: []byte(query.ServiceName),
				},
				{
					Key:   []byte("operation_name"),
					Value: []byte(query.OperationName),
				},
			},
		},
		Limit:     query.NumTraces,
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
	return traces, err
}

func (s *Store) FindTraceIDs(_ context.Context, query *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	deadline := searchutils.NewDeadline(time.Now(), time.Minute, "")

	spanNames, _, err := vmselect.SearchMetricNames(false, &storage.SearchQuery{
		MinTimestamp: timestampFromTime(query.StartTimeMin),
		MaxTimestamp: timestampFromTime(query.StartTimeMax),
		TagFilterss: [][]storage.TagFilter{
			{
				{
					Key:   []byte("service"),
					Value: []byte(query.ServiceName),
				},
				{
					Key:   []byte("operation_name"),
					Value: []byte(query.OperationName),
				},
			},
		},
		Limit:     query.NumTraces,
		Forward:   true,
		FetchData: storage.NotFetch,
	}, deadline)
	if err != nil {
		return nil, err
	}

	traceIDs := make([]model.TraceID, 0)
	for _, spanName := range spanNames {
		traceID, err := model.TraceIDFromBytes(spanName.TraceID)
		if err != nil {
			return nil, err
		}
		traceIDs = append(traceIDs, traceID)
	}
	return traceIDs, err
}

func (s *Store) WriteSpan(_ context.Context, span *model.Span) error {
	ctx := vminsert.GetInsertCtx()
	defer vminsert.PutInsertCtx(ctx)

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
