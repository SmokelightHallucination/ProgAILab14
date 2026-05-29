// Package config loads collector settings from environment variables so the
// same binary runs identically under docker-compose and Kubernetes.
package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	InstanceID    string        // unique id in the etcd membership set
	Source        string        // "synthetic" | "openaq"
	OpenAQKey     string        // X-API-Key when Source=openaq
	PollInterval  time.Duration // how often raw readings are fetched
	WindowSize    time.Duration // tumbling-window length
	EtcdEndpoints []string      // empty → standalone (no sharding)
	LeaseTTL      int64         // etcd lease seconds

	KafkaEnabled bool
	KafkaBrokers []string
	KafkaTopic   string

	ParquetEnabled bool
	ParquetPath    string

	FlightEnabled bool
	FlightAddr    string

	MetricsAddr string // host:port for /metrics and /healthz
}

// Load reads configuration from the environment, applying sensible defaults.
func Load() Config {
	host, _ := os.Hostname()
	return Config{
		InstanceID:     env("INSTANCE_ID", host),
		Source:         env("SOURCE", "synthetic"),
		OpenAQKey:      env("OPENAQ_API_KEY", ""),
		PollInterval:   envDuration("POLL_INTERVAL", 2*time.Second),
		WindowSize:     envDuration("WINDOW_SIZE", 10*time.Second),
		EtcdEndpoints:  envList("ETCD_ENDPOINTS", nil),
		LeaseTTL:       int64(envInt("ETCD_LEASE_TTL", 10)),
		KafkaEnabled:   envBool("KAFKA_ENABLED", false),
		KafkaBrokers:   envList("KAFKA_BROKERS", []string{"localhost:9092"}),
		KafkaTopic:     env("KAFKA_TOPIC", "air-quality.aggregates"),
		ParquetEnabled: envBool("PARQUET_ENABLED", true),
		ParquetPath:    env("PARQUET_PATH", "data/aggregates.parquet"),
		FlightEnabled:  envBool("FLIGHT_ENABLED", false),
		FlightAddr:     env("FLIGHT_ADDR", "0.0.0.0:8815"),
		MetricsAddr:    env("METRICS_ADDR", "0.0.0.0:9100"),
	}
}

func env(k, def string) string {
	if v, ok := os.LookupEnv(k); ok && v != "" {
		return v
	}
	return def
}

func envList(k string, def []string) []string {
	v, ok := os.LookupEnv(k)
	if !ok || v == "" {
		return def
	}
	parts := strings.Split(v, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

func envBool(k string, def bool) bool {
	if v, ok := os.LookupEnv(k); ok {
		b, err := strconv.ParseBool(v)
		if err == nil {
			return b
		}
	}
	return def
}

func envInt(k string, def int) int {
	if v, ok := os.LookupEnv(k); ok {
		n, err := strconv.Atoi(v)
		if err == nil {
			return n
		}
	}
	return def
}

func envDuration(k string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(k); ok {
		d, err := time.ParseDuration(v)
		if err == nil {
			return d
		}
	}
	return def
}
