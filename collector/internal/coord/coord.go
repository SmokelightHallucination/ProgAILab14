// Package coord distributes monitoring stations across collector instances
// (advanced task #1). Coordination is done through etcd: every instance
// registers itself under a lease, watches the live membership set, and
// deterministically claims a disjoint shard of stations. When membership
// changes (a pod scales in/out or dies and its lease expires) every instance
// recomputes its shard, so the catalogue stays fully covered with no overlap.
package coord

import (
	"airquality/collector/internal/model"
)

// Coordinator decides which stations the local instance is responsible for.
type Coordinator interface {
	// Owns reports whether this instance should poll the given station.
	Owns(stationID string) bool
	// Assigned filters a station catalogue down to this instance's shard.
	Assigned(stations []model.Station) []model.Station
	// Members returns the number of live collector instances (for metrics).
	Members() int
	// Close releases the etcd lease and stops background watching.
	Close() error
}

// Standalone is the no-etcd coordinator: a single instance owns everything.
type Standalone struct{}

func (Standalone) Owns(string) bool { return true }
func (Standalone) Assigned(s []model.Station) []model.Station { return s }
func (Standalone) Members() int { return 1 }
func (Standalone) Close() error { return nil }
