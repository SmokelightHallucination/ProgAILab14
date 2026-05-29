//! Core air-quality validation logic, shared by the cgo (`ffi`) and PyO3 (`py`)
//! bindings. No I/O, no platform dependencies — just the rules.
//!
//! A measurement is rejected when:
//!   * the pollutant parameter is unknown,
//!   * the value is non-finite (NaN/Inf) or negative,
//!   * the value falls outside the physically plausible range for that pollutant,
//!   * the unit does not match the expected unit for that pollutant,
//!   * the coordinates are outside the WGS84 bounds.

/// Outcome of validating a single measurement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Validity {
    Valid,
    Invalid(Reason),
}

/// Why a measurement failed validation. Stable discriminants are exposed across
/// the C ABI as integers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum Reason {
    UnknownParameter = 1,
    NonFinite = 2,
    OutOfRange = 3,
    BadUnit = 4,
    BadCoordinates = 5,
}

impl Reason {
    pub fn message(self) -> &'static str {
        match self {
            Reason::UnknownParameter => "unknown pollutant parameter",
            Reason::NonFinite => "value is NaN, infinite or negative",
            Reason::OutOfRange => "value outside plausible range for pollutant",
            Reason::BadUnit => "unit does not match expected unit",
            Reason::BadCoordinates => "latitude/longitude outside WGS84 bounds",
        }
    }
}

/// Expected unit and inclusive plausible concentration range per pollutant.
struct Spec {
    unit: &'static str,
    min: f64,
    max: f64,
}

fn spec(parameter: &str) -> Option<Spec> {
    let s = match parameter {
        "pm25" => Spec { unit: "µg/m³", min: 0.0, max: 1000.0 },
        "pm10" => Spec { unit: "µg/m³", min: 0.0, max: 2000.0 },
        "no2" => Spec { unit: "µg/m³", min: 0.0, max: 3000.0 },
        "o3" => Spec { unit: "µg/m³", min: 0.0, max: 1000.0 },
        "so2" => Spec { unit: "µg/m³", min: 0.0, max: 3000.0 },
        "co" => Spec { unit: "mg/m³", min: 0.0, max: 100.0 },
        _ => return None,
    };
    Some(s)
}

/// Validate one measurement. `unit` may be empty to skip the unit check
/// (some upstream sources omit it).
pub fn validate(
    parameter: &str,
    value: f64,
    unit: &str,
    latitude: f64,
    longitude: f64,
) -> Validity {
    let spec = match spec(parameter) {
        Some(s) => s,
        None => return Validity::Invalid(Reason::UnknownParameter),
    };
    if !value.is_finite() || value < 0.0 {
        return Validity::Invalid(Reason::NonFinite);
    }
    if value < spec.min || value > spec.max {
        return Validity::Invalid(Reason::OutOfRange);
    }
    if !unit.is_empty() && unit != spec.unit {
        return Validity::Invalid(Reason::BadUnit);
    }
    if !(-90.0..=90.0).contains(&latitude) || !(-180.0..=180.0).contains(&longitude) {
        return Validity::Invalid(Reason::BadCoordinates);
    }
    Validity::Valid
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_typical_reading() {
        assert_eq!(validate("pm25", 14.0, "µg/m³", 55.75, 37.6), Validity::Valid);
    }

    #[test]
    fn rejects_unknown_parameter() {
        assert_eq!(validate("xx", 1.0, "", 0.0, 0.0), Validity::Invalid(Reason::UnknownParameter));
    }

    #[test]
    fn rejects_spike() {
        assert_eq!(validate("pm25", 9000.0, "µg/m³", 0.0, 0.0), Validity::Invalid(Reason::OutOfRange));
    }

    #[test]
    fn rejects_nan_and_negative() {
        assert_eq!(validate("o3", f64::NAN, "", 0.0, 0.0), Validity::Invalid(Reason::NonFinite));
        assert_eq!(validate("o3", -1.0, "", 0.0, 0.0), Validity::Invalid(Reason::NonFinite));
    }

    #[test]
    fn rejects_bad_unit_and_coords() {
        assert_eq!(validate("co", 1.0, "µg/m³", 0.0, 0.0), Validity::Invalid(Reason::BadUnit));
        assert_eq!(validate("pm25", 10.0, "µg/m³", 200.0, 0.0), Validity::Invalid(Reason::BadCoordinates));
    }
}
