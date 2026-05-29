// Package metrics exposes Prometheus counters/gauges for the collector. The
// queue-length gauge (aq_queue_length) is what the Kubernetes HPA scales on
// (advanced task #5), alongside CPU.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// MeasurementsTotal counts raw readings pulled from the source.
	MeasurementsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "aq_measurements_total",
		Help: "Total raw measurements fetched from the source.",
	})
	// InvalidTotal counts readings rejected by the Rust validator, by reason.
	InvalidTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "aq_invalid_total",
		Help: "Measurements rejected by validation, labelled by reason.",
	}, []string{"reason"})
	// AggregatesTotal counts window aggregates emitted downstream.
	AggregatesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "aq_aggregates_total",
		Help: "Total window aggregates produced and published.",
	})
	// QueueLength is the current depth of the internal sink queue — the HPA
	// scaling signal.
	QueueLength = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "aq_queue_length",
		Help: "Current number of aggregates buffered awaiting publish.",
	})
	// AssignedStations is how many stations this instance owns (shard size).
	AssignedStations = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "aq_assigned_stations",
		Help: "Number of stations assigned to this collector instance.",
	})
	// Members is the number of live collector instances per etcd.
	Members = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "aq_cluster_members",
		Help: "Number of live collector instances seen via etcd.",
	})
)

// Handler returns the HTTP handler serving /metrics.
func Handler() http.Handler { return promhttp.Handler() }
