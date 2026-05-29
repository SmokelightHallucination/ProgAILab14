// Command gensample produces a deterministic sample Parquet of window
// aggregates without any external services (etcd/Kafka). It reuses the real
// source → validate → tumbling-window → Parquet path, so the Python analyzer
// and CI have data to work with offline.
//
//	go run ./cmd/gensample -out ../data/aggregates.parquet -windows 30
package main

import (
	"context"
	"flag"
	"log"
	"time"

	"airquality/collector/internal/sink"
	"airquality/collector/internal/source"
	"airquality/collector/internal/validate"
	"airquality/collector/internal/window"
)

func main() {
	out := flag.String("out", "data/aggregates.parquet", "output Parquet path")
	windows := flag.Int("windows", 30, "number of tumbling windows to emit")
	perWindow := flag.Int("samples", 5, "raw samples per window per station")
	flag.Parse()

	src := source.NewSynthetic()
	ctx := context.Background()
	stations, err := src.Stations(ctx)
	if err != nil {
		log.Fatal(err)
	}
	ids := make([]string, len(stations))
	for i, s := range stations {
		ids[i] = s.ID
	}

	ps, err := sink.NewParquet(*out)
	if err != nil {
		log.Fatal(err)
	}

	// Each iteration is one tumbling window covering `samples` readings.
	now := time.Now().UTC().Add(-time.Duration(*windows) * 10 * time.Second)
	total := 0
	for w := 0; w < *windows; w++ {
		win := window.New(time.Hour) // manual flush per iteration
		for s := 0; s < *perWindow; s++ {
			ms, err := src.Fetch(ctx, ids)
			if err != nil {
				log.Fatal(err)
			}
			for _, m := range ms {
				if validate.Check(m).OK {
					win.Add(m)
				}
			}
		}
		aggs := win.Flush()
		// Backdate window timestamps so the time-series chart spans a range.
		ws := now.Add(time.Duration(w) * 10 * time.Second)
		for i := range aggs {
			aggs[i].WindowStart = ws
			aggs[i].WindowEnd = ws.Add(10 * time.Second)
		}
		if err := ps.Publish(ctx, aggs); err != nil {
			log.Fatal(err)
		}
		total += len(aggs)
	}
	if err := ps.Close(); err != nil {
		log.Fatal(err)
	}
	log.Printf("wrote %d aggregates across %d windows to %s", total, *windows, *out)
}
