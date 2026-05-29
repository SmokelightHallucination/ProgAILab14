// Package source abstracts where air-quality measurements come from. The
// pipeline ships two implementations: a synthetic generator (fully offline,
// used by docker-compose and CI) and an OpenAQ API client (variant 20's real
// data source). Both yield the same model.Measurement values.
package source

import (
	"context"

	"airquality/collector/internal/model"
)

// Source produces measurements for the set of stations assigned to this
// collector instance. Stations returns the catalogue the coordinator shards
// over; Fetch returns the latest reading for every station in ids.
type Source interface {
	// Name identifies the source in logs and metrics.
	Name() string
	// Stations returns the full catalogue of monitoring stations.
	Stations(ctx context.Context) ([]model.Station, error)
	// Fetch returns one measurement per (station, parameter) for the given
	// station ids — the instantaneous snapshot a tumbling window rolls up.
	Fetch(ctx context.Context, ids []string) ([]model.Measurement, error)
}
