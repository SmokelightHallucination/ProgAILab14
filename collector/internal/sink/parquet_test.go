package sink

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"airquality/collector/internal/model"
)

func TestParquetSinkRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agg.parquet")
	ps, err := NewParquet(path)
	if err != nil {
		t.Fatalf("NewParquet: %v", err)
	}

	now := time.Now().UTC()
	aggs := []model.Aggregate{
		{StationID: "st-1", Parameter: "pm25", Unit: "µg/m³", Avg: 14.2, Count: 5,
			WindowStart: now, WindowEnd: now.Add(time.Second), AQI: 55, Category: "Moderate"},
		{StationID: "st-2", Parameter: "o3", Unit: "µg/m³", Avg: 60, Count: 5,
			WindowStart: now, WindowEnd: now.Add(time.Second), AQI: 30, Category: "Good"},
	}
	if err := ps.Publish(context.Background(), aggs); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	// Second window → second row group.
	if err := ps.Publish(context.Background(), aggs); err != nil {
		t.Fatalf("Publish 2: %v", err)
	}
	if err := ps.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	rdr, err := file.NewParquetReader(f)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer rdr.Close()

	arrowRdr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("arrow reader: %v", err)
	}
	tbl, err := arrowRdr.ReadTable(context.Background())
	if err != nil {
		t.Fatalf("read table: %v", err)
	}
	defer tbl.Release()

	if got := tbl.NumRows(); got != 4 {
		t.Fatalf("expected 4 rows (2 windows × 2 aggregates), got %d", got)
	}
	if got := tbl.NumCols(); got != 17 {
		t.Fatalf("expected 17 columns, got %d", got)
	}
}
