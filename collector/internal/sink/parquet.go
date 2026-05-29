package sink

import (
	"context"
	"os"
	"sync"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"airquality/collector/internal/arrowutil"
	"airquality/collector/internal/model"
)

// ParquetSink appends each window's aggregates as a row group to a single
// Snappy-compressed Parquet file. Python reads it directly with Polars/DuckDB
// (the JSON → Parquet upgrade from the base assignment).
type ParquetSink struct {
	mu     sync.Mutex
	f      *os.File
	writer *pqarrow.FileWriter
	mem    memory.Allocator
}

// NewParquet opens (truncating) the target Parquet file and prepares the writer.
func NewParquet(path string) (*ParquetSink, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	w, err := pqarrow.NewFileWriter(arrowutil.Schema, f, props, pqarrow.DefaultWriterProps())
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &ParquetSink{f: f, writer: w, mem: memory.DefaultAllocator}, nil
}

func (p *ParquetSink) Name() string { return "parquet" }

func (p *ParquetSink) Publish(_ context.Context, aggs []model.Aggregate) error {
	if len(aggs) == 0 {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	rec := arrowutil.BuildRecord(aggs, p.mem)
	defer rec.Release()
	return p.writer.WriteBuffered(rec)
}

func (p *ParquetSink) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	// pqarrow.FileWriter.Close also closes the underlying *os.File, so we must
	// not close it again here.
	return p.writer.Close()
}
