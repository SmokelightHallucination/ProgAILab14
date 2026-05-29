//go:build !rustvalidate

package validate

import (
	"math"

	"airquality/collector/internal/model"
)

const backend = "go-native"

type spec struct {
	unit     string
	min, max float64
}

var specs = map[string]spec{
	model.ParamPM25: {"µg/m³", 0, 1000},
	model.ParamPM10: {"µg/m³", 0, 2000},
	model.ParamNO2:  {"µg/m³", 0, 3000},
	model.ParamO3:   {"µg/m³", 0, 1000},
	model.ParamSO2:  {"µg/m³", 0, 3000},
	model.ParamCO:   {"mg/m³", 0, 100},
}

// check is the pure-Go mirror of validator/core/src/lib.rs.
func check(m model.Measurement) Result {
	sp, ok := specs[m.Parameter]
	if !ok {
		return Result{false, 1, "unknown pollutant parameter"}
	}
	if math.IsNaN(m.Value) || math.IsInf(m.Value, 0) || m.Value < 0 {
		return Result{false, 2, "value is NaN, infinite or negative"}
	}
	if m.Value < sp.min || m.Value > sp.max {
		return Result{false, 3, "value outside plausible range for pollutant"}
	}
	if m.Unit != "" && m.Unit != sp.unit {
		return Result{false, 4, "unit does not match expected unit"}
	}
	if m.Latitude < -90 || m.Latitude > 90 || m.Longitude < -180 || m.Longitude > 180 {
		return Result{false, 5, "latitude/longitude outside WGS84 bounds"}
	}
	return Result{true, 0, "valid"}
}
