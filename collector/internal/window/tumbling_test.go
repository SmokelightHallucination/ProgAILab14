package window

import (
	"testing"
	"time"

	"airquality/collector/internal/model"
)

func TestTumblingAggregates(t *testing.T) {
	w := New(time.Hour) // long window; we flush manually
	base := model.Measurement{StationID: "st-1", Station: "S1", Parameter: "pm25", Unit: "µg/m³"}

	for _, v := range []float64{10, 20, 30} {
		m := base
		m.Value = v
		w.Add(m)
	}
	// A different parameter on the same station is a separate bucket.
	o := base
	o.Parameter, o.Value = "o3", 50
	w.Add(o)

	aggs := w.Flush()
	if len(aggs) != 2 {
		t.Fatalf("expected 2 buckets, got %d", len(aggs))
	}

	byParam := map[string]model.Aggregate{}
	for _, a := range aggs {
		byParam[a.Parameter] = a
	}
	pm := byParam["pm25"]
	if pm.Count != 3 || pm.Sum != 60 || pm.Avg != 20 || pm.Min != 10 || pm.Max != 30 {
		t.Fatalf("pm25 aggregate wrong: %+v", pm)
	}
	if pm.AQI <= 0 || pm.Category == "" {
		t.Fatalf("expected AQI/category to be set: %+v", pm)
	}

	// After flush the window is empty.
	if got := w.Flush(); len(got) != 0 {
		t.Fatalf("expected empty window after flush, got %d", len(got))
	}
}
