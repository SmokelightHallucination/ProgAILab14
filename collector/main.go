// Command collector is the distributed Go air-quality collector (variant 20).
//
// Pipeline role:
//
//	OpenAQ/synthetic source → validate (Rust) → tumbling window aggregation
//	→ fan-out sinks (Kafka stream / Parquet file / Arrow Flight) → Python.
//
// Several instances coordinate through etcd: each owns a disjoint shard of
// monitoring stations (rendezvous hashing). Prometheus metrics (notably the
// queue-length gauge) drive the Kubernetes HPA. Handles SIGINT/SIGTERM with a
// graceful flush.
package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"airquality/collector/internal/config"
	"airquality/collector/internal/coord"
	"airquality/collector/internal/metrics"
	"airquality/collector/internal/model"
	"airquality/collector/internal/sink"
	"airquality/collector/internal/source"
	"airquality/collector/internal/validate"
	"airquality/collector/internal/window"
)

func main() {
	cfg := config.Load()
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("[" + cfg.InstanceID + "] ")
	log.Printf("starting collector: source=%s window=%s poll=%s validator=%s",
		cfg.Source, cfg.WindowSize, cfg.PollInterval, validate.Backend())

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	src := buildSource(cfg)
	coordinator := buildCoordinator(ctx, cfg)
	defer coordinator.Close()

	sinks := buildSinks(cfg)
	defer closeSinks(sinks)

	startMetricsServer(cfg.MetricsAddr)

	// Aggregates flow through a buffered queue to the publisher goroutine; the
	// queue depth is the HPA scaling signal.
	queue := make(chan []model.Aggregate, 256)
	go publisher(ctx, queue, sinks)

	run(ctx, cfg, src, coordinator, queue)

	// Graceful shutdown: let the publisher drain, then sinks close via defer.
	close(queue)
	time.Sleep(500 * time.Millisecond)
	log.Printf("shutdown complete")
}

// run is the main collection loop.
func run(ctx context.Context, cfg config.Config, src source.Source, c coord.Coordinator, queue chan<- []model.Aggregate) {
	win := window.New(cfg.WindowSize)

	catalogue, err := src.Stations(ctx)
	if err != nil {
		log.Fatalf("failed to load stations: %v", err)
	}
	assigned := c.Assigned(catalogue)
	logAssignment(c, len(assigned), len(catalogue))

	poll := time.NewTicker(cfg.PollInterval)
	defer poll.Stop()
	// Re-evaluate shard ownership periodically so scale events take effect.
	reassign := time.NewTicker(5 * time.Second)
	defer reassign.Stop()

	for {
		select {
		case <-ctx.Done():
			if final := win.Flush(); len(final) > 0 {
				log.Printf("flushing final window: %d aggregates", len(final))
				queue <- final
			}
			return

		case <-reassign.C:
			assigned = c.Assigned(catalogue)
			logAssignment(c, len(assigned), len(catalogue))

		case <-poll.C:
			ids := stationIDs(assigned)
			measurements, err := src.Fetch(ctx, ids)
			if err != nil {
				log.Printf("fetch error: %v", err)
				continue
			}
			for _, m := range measurements {
				metrics.MeasurementsTotal.Inc()
				if res := validate.Check(m); !res.OK {
					metrics.InvalidTotal.WithLabelValues(res.Reason).Inc()
					continue
				}
				win.Add(m)
			}
			if win.Due() {
				if aggs := win.Flush(); len(aggs) > 0 {
					metrics.AggregatesTotal.Add(float64(len(aggs)))
					select {
					case queue <- aggs:
						metrics.QueueLength.Set(float64(len(queue)))
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}
}

// publisher fans each batch out to all configured sinks.
func publisher(ctx context.Context, queue <-chan []model.Aggregate, sinks []sink.Sink) {
	for aggs := range queue {
		metrics.QueueLength.Set(float64(len(queue)))
		for _, s := range sinks {
			// Use a fresh timeout so a slow sink can't wedge shutdown forever.
			pctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := s.Publish(pctx, aggs); err != nil {
				log.Printf("sink %s publish error: %v", s.Name(), err)
			}
			cancel()
		}
		_ = ctx
	}
	metrics.QueueLength.Set(0)
}

func buildSource(cfg config.Config) source.Source {
	switch cfg.Source {
	case "openaq":
		log.Printf("using OpenAQ API source")
		return source.NewOpenAQ(cfg.OpenAQKey, nil)
	default:
		log.Printf("using synthetic source")
		return source.NewSynthetic()
	}
}

func buildCoordinator(ctx context.Context, cfg config.Config) coord.Coordinator {
	if len(cfg.EtcdEndpoints) == 0 {
		log.Printf("etcd disabled → standalone (this instance owns all stations)")
		return coord.Standalone{}
	}
	c, err := coord.NewEtcd(ctx, cfg.EtcdEndpoints, cfg.InstanceID, cfg.LeaseTTL)
	if err != nil {
		log.Fatalf("etcd coordination failed: %v", err)
	}
	return c
}

func buildSinks(cfg config.Config) []sink.Sink {
	var sinks []sink.Sink
	if cfg.KafkaEnabled {
		sinks = append(sinks, sink.NewKafka(cfg.KafkaBrokers, cfg.KafkaTopic))
		log.Printf("sink enabled: kafka topic=%s brokers=%v", cfg.KafkaTopic, cfg.KafkaBrokers)
	}
	if cfg.ParquetEnabled {
		ps, err := sink.NewParquet(cfg.ParquetPath)
		if err != nil {
			log.Fatalf("parquet sink: %v", err)
		}
		sinks = append(sinks, ps)
		log.Printf("sink enabled: parquet path=%s", cfg.ParquetPath)
	}
	if cfg.FlightEnabled {
		fs, err := sink.NewFlight(cfg.FlightAddr, 64)
		if err != nil {
			log.Fatalf("flight sink: %v", err)
		}
		sinks = append(sinks, fs)
		log.Printf("sink enabled: arrow flight addr=%s", cfg.FlightAddr)
	}
	if len(sinks) == 0 {
		log.Printf("warning: no sinks enabled, aggregates will be discarded")
	}
	return sinks
}

func closeSinks(sinks []sink.Sink) {
	for _, s := range sinks {
		if err := s.Close(); err != nil {
			log.Printf("closing sink %s: %v", s.Name(), err)
		}
	}
}

func startMetricsServer(addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		log.Printf("metrics on http://%s/metrics", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("metrics server error: %v", err)
		}
	}()
}

func stationIDs(stations []model.Station) []string {
	ids := make([]string, len(stations))
	for i, s := range stations {
		ids[i] = s.ID
	}
	return ids
}

func logAssignment(c coord.Coordinator, assigned, total int) {
	metrics.AssignedStations.Set(float64(assigned))
	metrics.Members.Set(float64(c.Members()))
	log.Printf("shard: %d/%d stations assigned (cluster members=%d)",
		assigned, total, c.Members())
}
