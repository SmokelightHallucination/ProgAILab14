// Package sink delivers window aggregates to downstream consumers. The
// collector can fan out to several sinks at once (Kafka for streaming, Parquet
// for batch analysis, Arrow Flight for zero-copy pulls).
package sink

import (
	"context"

	"airquality/collector/internal/model"
)

// Sink publishes a batch of aggregates. Implementations must be safe to call
// from the single collector loop.
type Sink interface {
	Name() string
	Publish(ctx context.Context, aggs []model.Aggregate) error
	Close() error
}
