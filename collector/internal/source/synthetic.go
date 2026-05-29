package source

import (
	"context"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"sync"
	"time"

	"airquality/collector/internal/model"
)

// stationSeed defines a real-world city the synthetic generator emulates.
type stationSeed struct {
	name, city, country string
	lat, lon            float64
	baseline            map[string]float64 // typical concentration per pollutant
}

// A small but globally spread catalogue so sharding across instances is
// visible. Baselines are loosely realistic annual means.
var seeds = []stationSeed{
	{"Moscow Centre", "Moscow", "RU", 55.751, 37.618, map[string]float64{"pm25": 14, "pm10": 28, "no2": 45, "o3": 60, "so2": 8, "co": 1.2}},
	{"Beijing Chaoyang", "Beijing", "CN", 39.921, 116.443, map[string]float64{"pm25": 58, "pm10": 92, "no2": 55, "o3": 70, "so2": 14, "co": 1.8}},
	{"Delhi Anand Vihar", "Delhi", "IN", 28.650, 77.316, map[string]float64{"pm25": 95, "pm10": 160, "no2": 60, "o3": 45, "so2": 18, "co": 2.4}},
	{"Los Angeles N. Main", "Los Angeles", "US", 34.066, -118.227, map[string]float64{"pm25": 12, "pm10": 30, "no2": 38, "o3": 95, "so2": 4, "co": 0.9}},
	{"London Marylebone", "London", "GB", 51.522, -0.155, map[string]float64{"pm25": 13, "pm10": 24, "no2": 70, "o3": 50, "so2": 5, "co": 0.7}},
	{"Paris Châtelet", "Paris", "FR", 48.862, 2.347, map[string]float64{"pm25": 15, "pm10": 26, "no2": 52, "o3": 55, "so2": 6, "co": 0.8}},
	{"Tokyo Shinjuku", "Tokyo", "JP", 35.690, 139.700, map[string]float64{"pm25": 11, "pm10": 21, "no2": 40, "o3": 48, "so2": 4, "co": 0.6}},
	{"Mexico City Merced", "Mexico City", "MX", 19.424, -99.119, map[string]float64{"pm25": 24, "pm10": 48, "no2": 50, "o3": 80, "so2": 9, "co": 1.5}},
	{"São Paulo Ibirapuera", "São Paulo", "BR", -23.591, -46.660, map[string]float64{"pm25": 18, "pm10": 35, "no2": 44, "o3": 65, "so2": 7, "co": 1.0}},
	{"Cairo Maadi", "Cairo", "EG", 29.960, 31.276, map[string]float64{"pm25": 75, "pm10": 140, "no2": 58, "o3": 40, "so2": 20, "co": 2.0}},
	{"Sydney Rozelle", "Sydney", "AU", -33.864, 151.171, map[string]float64{"pm25": 8, "pm10": 18, "no2": 28, "o3": 52, "so2": 3, "co": 0.5}},
	{"Krakow Aleja", "Krakow", "PL", 50.058, 19.926, map[string]float64{"pm25": 32, "pm10": 55, "no2": 48, "o3": 42, "so2": 11, "co": 1.3}},
}

// SyntheticSource generates physically-plausible readings: each pollutant
// oscillates around its baseline with a diurnal cycle plus gaussian noise, and
// rarely emits an out-of-range spike so the Rust validator has something to
// catch.
type SyntheticSource struct {
	mu       sync.Mutex
	rng      *rand.Rand
	stations []model.Station
	byID     map[string]stationSeed
}

// NewSynthetic builds the generator from the built-in station catalogue.
func NewSynthetic() *SyntheticSource {
	s := &SyntheticSource{
		rng:   rand.New(rand.NewSource(time.Now().UnixNano())),
		byID:  make(map[string]stationSeed),
	}
	for _, seed := range seeds {
		id := stationID(seed.name)
		s.stations = append(s.stations, model.Station{
			ID: id, Name: seed.name, City: seed.city, Country: seed.country,
			Latitude: seed.lat, Longitude: seed.lon,
		})
		s.byID[id] = seed
	}
	return s
}

func (s *SyntheticSource) Name() string { return "synthetic" }

func (s *SyntheticSource) Stations(context.Context) ([]model.Station, error) {
	return s.stations, nil
}

func (s *SyntheticSource) Fetch(_ context.Context, ids []string) ([]model.Measurement, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	// Diurnal factor: pollution peaks at rush hours (~08:00 and ~19:00).
	hour := float64(now.Hour()) + float64(now.Minute())/60.0
	diurnal := 1.0 + 0.35*math.Cos((hour-8)/24*2*math.Pi) + 0.25*math.Cos((hour-19)/12*2*math.Pi)

	var out []model.Measurement
	for _, id := range ids {
		seed, ok := s.byID[id]
		if !ok {
			continue
		}
		for _, p := range model.Parameters {
			base := seed.baseline[p]
			noise := s.rng.NormFloat64() * base * 0.15
			val := math.Max(0, base*diurnal+noise)
			// ~0.5% of readings are corrupt spikes for the validator to reject.
			if s.rng.Float64() < 0.005 {
				val = base * 50
			}
			out = append(out, model.Measurement{
				StationID: id, Station: seed.name, City: seed.city, Country: seed.country,
				Parameter: p, Value: round2(val), Unit: model.Units[p],
				Latitude: seed.lat, Longitude: seed.lon, Timestamp: now,
			})
		}
	}
	return out, nil
}

func round2(f float64) float64 { return math.Round(f*100) / 100 }

// stationID is a stable slug derived from the station name so ids survive
// restarts (etcd shard assignment depends on this).
func stationID(name string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(name))
	return fmt.Sprintf("st-%08x", h.Sum32())
}
