package grpc

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
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

var rowsInserted = metrics.NewCounter(`vm_rows_inserted_total{type="importer"}`)

type Store struct {
}

func (s *Store) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	return nil, nil
}

func (s *Store) GetServices(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (s *Store) GetOperations(_ context.Context, query spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	labelName := query.ServiceName
	deadline := searchutils.NewDeadline(time.Now(), time.Minute, "")

	labelValues, _, err := vmselect.GetLabelValues(false, labelName, deadline)
	if err != nil {
		return nil, fmt.Errorf(`cannot obtain label values for %q: %w`, labelName, err)
	}
	operations := make([]spanstore.Operation, 0)
	for _, value := range labelValues {
		operations = append(operations, spanstore.Operation{
			Name: value,
		})
	}
	return operations, nil
}

func (s *Store) FindTraces(ctx context.Context, query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	return nil, nil
}

func (s *Store) FindTraceIDs(ctx context.Context, query *spanstore.TraceQueryParameters) ([]model.TraceID, error) {

	return nil, nil
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

	// Assume that all the rows for a single connection belong to the same (AccountID, ProjectID).
	rowsInserted.Add(1)
	return nil
}

func (s *Store) GetDependencies(ctx context.Context, endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	return nil, nil
}

func (s *Store) WriteDependencies(ts time.Time, dependencies []model.DependencyLink) error {
	return nil
}
