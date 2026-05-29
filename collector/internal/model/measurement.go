// Package model describes the air-quality domain types shared across the
// collector: a single sensor measurement and the windowed aggregate that the
// collector emits downstream (Kafka / Arrow Flight / Parquet).
package model

import (
	"math"
	"time"
)

// Pollutant parameters tracked by the air-quality pipeline (OpenAQ naming).
const (
	ParamPM25 = "pm25" // particulate matter < 2.5 µm,  µg/m³
	ParamPM10 = "pm10" // particulate matter < 10 µm,   µg/m³
	ParamNO2  = "no2"  // nitrogen dioxide,              µg/m³
	ParamO3   = "o3"   // ozone,                         µg/m³
	ParamSO2  = "so2"  // sulfur dioxide,                µg/m³
	ParamCO   = "co"   // carbon monoxide,               mg/m³
)

// Parameters is the canonical ordered list of pollutants the collector reads.
var Parameters = []string{ParamPM25, ParamPM10, ParamNO2, ParamO3, ParamSO2, ParamCO}

// Units maps each parameter to its physical unit of measure.
var Units = map[string]string{
	ParamPM25: "µg/m³",
	ParamPM10: "µg/m³",
	ParamNO2:  "µg/m³",
	ParamO3:   "µg/m³",
	ParamSO2:  "µg/m³",
	ParamCO:   "mg/m³",
}

// Station is an air-quality monitoring location. Stations are the unit of
// sharding: each collector instance owns a disjoint subset of stations.
type Station struct {
	ID        string  `json:"location_id"`
	Name      string  `json:"location"`
	City      string  `json:"city"`
	Country   string  `json:"country"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// Measurement is a single pollutant reading from one station at one instant.
type Measurement struct {
	StationID string    `json:"location_id"`
	Station   string    `json:"location"`
	City      string    `json:"city"`
	Country   string    `json:"country"`
	Parameter string    `json:"parameter"`
	Value     float64   `json:"value"`
	Unit      string    `json:"unit"`
	Latitude  float64   `json:"latitude"`
	Longitude float64   `json:"longitude"`
	Timestamp time.Time `json:"timestamp"`
}

// Aggregate is the tumbling-window roll-up the collector sends to Python
// instead of raw measurements. One row per (station, parameter, window).
type Aggregate struct {
	StationID  string    `json:"location_id"`
	Station    string    `json:"location"`
	City       string    `json:"city"`
	Country    string    `json:"country"`
	Parameter  string    `json:"parameter"`
	Unit       string    `json:"unit"`
	Latitude   float64   `json:"latitude"`
	Longitude  float64   `json:"longitude"`
	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`
	Count      int64     `json:"count"`
	Sum        float64   `json:"sum"`
	Avg        float64   `json:"avg"`
	Min        float64   `json:"min"`
	Max        float64   `json:"max"`
	AQI        int       `json:"aqi"`         // US EPA AQI for the window average
	Category   string    `json:"aqi_category"`
}

// aqiBreak is one row of an EPA AQI breakpoint table.
type aqiBreak struct {
	cLow, cHigh float64 // concentration range
	iLow, iHigh int     // AQI range
}

// US EPA breakpoint tables (24h averages for PM, 1h/8h for gases — simplified
// here to a single table per pollutant which is enough for the demo dashboard).
var aqiTables = map[string][]aqiBreak{
	ParamPM25: {
		{0.0, 12.0, 0, 50}, {12.1, 35.4, 51, 100}, {35.5, 55.4, 101, 150},
		{55.5, 150.4, 151, 200}, {150.5, 250.4, 201, 300}, {250.5, 500.4, 301, 500},
	},
	ParamPM10: {
		{0, 54, 0, 50}, {55, 154, 51, 100}, {155, 254, 101, 150},
		{255, 354, 151, 200}, {355, 424, 201, 300}, {425, 604, 301, 500},
	},
	ParamO3: { // µg/m³ (8h), coarse mapping
		{0, 100, 0, 50}, {101, 160, 51, 100}, {161, 215, 101, 150},
		{216, 265, 151, 200}, {266, 800, 201, 300},
	},
	ParamNO2: {
		{0, 100, 0, 50}, {101, 200, 51, 100}, {201, 700, 101, 150},
		{701, 1200, 151, 200}, {1201, 2350, 201, 300},
	},
	ParamSO2: {
		{0, 100, 0, 50}, {101, 200, 51, 100}, {201, 500, 101, 150},
		{501, 1000, 151, 200}, {1001, 2000, 201, 300},
	},
	ParamCO: { // mg/m³
		{0, 5, 0, 50}, {5.1, 10, 51, 100}, {10.1, 14, 101, 150},
		{14.1, 17, 151, 200}, {17.1, 34, 201, 300},
	},
}

// AQI converts a pollutant concentration to a US EPA Air Quality Index value
// via piecewise-linear interpolation. Returns -1 when no table is available.
func AQI(parameter string, concentration float64) int {
	table, ok := aqiTables[parameter]
	if !ok {
		return -1
	}
	c := concentration
	for _, b := range table {
		if c >= b.cLow && c <= b.cHigh {
			aqi := float64(b.iHigh-b.iLow)/(b.cHigh-b.cLow)*(c-b.cLow) + float64(b.iLow)
			return int(math.Round(aqi))
		}
	}
	// Above the highest breakpoint → cap at the table maximum.
	last := table[len(table)-1]
	return last.iHigh
}

// AQICategory maps an AQI value to its human-readable health category.
func AQICategory(aqi int) string {
	switch {
	case aqi < 0:
		return "Unknown"
	case aqi <= 50:
		return "Good"
	case aqi <= 100:
		return "Moderate"
	case aqi <= 150:
		return "Unhealthy for Sensitive Groups"
	case aqi <= 200:
		return "Unhealthy"
	case aqi <= 300:
		return "Very Unhealthy"
	default:
		return "Hazardous"
	}
}
