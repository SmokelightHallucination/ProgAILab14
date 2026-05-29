package sink

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"

	"airquality/collector/internal/model"
)

// KafkaSink streams aggregates to a Kafka topic, one JSON message per
// aggregate keyed by station id (advanced task: Kafka streaming). The Python
// analyzer and the Streamlit dashboard both consume this topic.
type KafkaSink struct {
	w *kafka.Writer
}

// NewKafka creates the producer. The topic is auto-created if the broker allows
// it; docker-compose pre-creates it as well.
func NewKafka(brokers []string, topic string) *KafkaSink {
	return &KafkaSink{
		w: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  topic,
			Balancer:               &kafka.Hash{}, // same station → same partition (ordering)
			BatchTimeout:           200 * time.Millisecond,
			RequiredAcks:           kafka.RequireOne,
			AllowAutoTopicCreation: true, // create the topic on first publish (no race)
		},
	}
}

func (k *KafkaSink) Name() string { return "kafka" }

func (k *KafkaSink) Publish(ctx context.Context, aggs []model.Aggregate) error {
	msgs := make([]kafka.Message, 0, len(aggs))
	for _, a := range aggs {
		payload, err := json.Marshal(a)
		if err != nil {
			return err
		}
		msgs = append(msgs, kafka.Message{Key: []byte(a.StationID), Value: payload})
	}
	return k.w.WriteMessages(ctx, msgs...)
}

func (k *KafkaSink) Close() error { return k.w.Close() }
