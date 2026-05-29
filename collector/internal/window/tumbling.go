// Package window implements the tumbling-window aggregation the collector runs
// before sending data to Python (advanced task #2). Raw measurements are folded
// into per-(station,parameter) aggregates over fixed, non-overlapping windows;
// only the aggregates cross the wire, cutting payload volume by ~Nx where N is
// the number of raw readings per window.
package window

import (
	"time"

	"airquality/collector/internal/model"
)

// key identifies the bucket a measurement belongs to within a window.
type key struct {
	station, parameter string
}

// Tumbling accumulates measurements and flushes them on a fixed interval.
// It is not safe for concurrent use; drive it from a single goroutine.
type Tumbling struct {
	dur     time.Duration
	start   time.Time
	buckets map[key]*acc
}

type acc struct {
	m            model.Measurement
	count        int64
	sum, min, max float64
}

// New creates a tumbling window of the given duration.
func New(d time.Duration) *Tumbling {
	return &Tumbling{dur: d, start: time.Now().UTC(), buckets: map[key]*acc{}}
}

// Add folds a single measurement into the current window.
func (t *Tumbling) Add(m model.Measurement) {
	k := key{m.StationID, m.Parameter}
	a, ok := t.buckets[k]
	if !ok {
		a = &acc{m: m, min: m.Value, max: m.Value}
		t.buckets[k] = a
	}
	a.count++
	a.sum += m.Value
	if m.Value < a.min {
		a.min = m.Value
	}
	if m.Value > a.max {
		a.max = m.Value
	}
}

// Due reports whether the current window has elapsed and should be flushed.
func (t *Tumbling) Due() bool { return time.Since(t.start) >= t.dur }

// Flush closes the current window, returns its aggregates, and opens the next
// one. Returns nil when the window is empty.
func (t *Tumbling) Flush() []model.Aggregate {
	end := time.Now().UTC()
	out := make([]model.Aggregate, 0, len(t.buckets))
	for k, a := range t.buckets {
		avg := a.sum / float64(a.count)
		aqi := model.AQI(k.parameter, avg)
		out = append(out, model.Aggregate{
			StationID: a.m.StationID, Station: a.m.Station, City: a.m.City,
			Country: a.m.Country, Parameter: k.parameter, Unit: a.m.Unit,
			Latitude: a.m.Latitude, Longitude: a.m.Longitude,
			WindowStart: t.start, WindowEnd: end,
			Count: a.count, Sum: round2(a.sum), Avg: round2(avg),
			Min: a.min, Max: a.max, AQI: aqi, Category: model.AQICategory(aqi),
		})
	}
	t.start = end
	t.buckets = map[key]*acc{}
	return out
}

func round2(f float64) float64 {
	return float64(int64(f*100+0.5)) / 100
}
