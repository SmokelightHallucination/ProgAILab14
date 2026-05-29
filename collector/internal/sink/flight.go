package sink

import (
	"context"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"airquality/collector/internal/arrowutil"
	"airquality/collector/internal/model"
)

// FlightSink is both a sink and an Apache Arrow Flight server (advanced task
// #3). Each published window is kept as an Arrow record in a bounded in-memory
// ring; a Python Flight client pulls them with DoGet for zero-copy, columnar
// transfer — no JSON, far less bytes than the original file hand-off.
type FlightSink struct {
	flight.BaseFlightServer
	srv flight.Server

	mu      sync.Mutex
	records []arrow.Record // most-recent-last ring buffer
	cap     int
	mem     memory.Allocator
}

// NewFlight starts the Flight gRPC server on addr (e.g. "0.0.0.0:8815") and
// keeps up to capacity recent window records available for pulling.
func NewFlight(addr string, capacity int) (*FlightSink, error) {
	s := &FlightSink{cap: capacity, mem: memory.DefaultAllocator}
	srv := flight.NewServerWithMiddleware(nil)
	if err := srv.Init(addr); err != nil {
		return nil, err
	}
	srv.RegisterFlightService(s)
	s.srv = srv
	go func() { _ = srv.Serve() }()
	return s, nil
}

func (f *FlightSink) Name() string { return "flight" }

func (f *FlightSink) Publish(_ context.Context, aggs []model.Aggregate) error {
	if len(aggs) == 0 {
		return nil
	}
	rec := arrowutil.BuildRecord(aggs, f.mem)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.records = append(f.records, rec)
	for len(f.records) > f.cap {
		f.records[0].Release()
		f.records = f.records[1:]
	}
	return nil
}

// DoGet streams every buffered record to the client. The ticket is ignored —
// there is a single logical stream ("aggregates").
func (f *FlightSink) DoGet(_ *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	f.mu.Lock()
	snapshot := make([]arrow.Record, len(f.records))
	for i, r := range f.records {
		r.Retain()
		snapshot[i] = r
	}
	f.mu.Unlock()

	w := flight.NewRecordWriter(stream, ipc.WithSchema(arrowutil.Schema))
	defer w.Close()
	for _, r := range snapshot {
		if err := w.Write(r); err != nil {
			r.Release()
			return err
		}
		r.Release()
	}
	return nil
}

// GetFlightInfo advertises the single stream and its schema.
func (f *FlightSink) GetFlightInfo(_ context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(arrowutil.Schema, f.mem),
		FlightDescriptor: desc,
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: []byte("aggregates")}}},
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

func (f *FlightSink) Close() error {
	f.srv.Shutdown()
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range f.records {
		r.Release()
	}
	f.records = nil
	return nil
}
