//! PyO3 bindings exposing the air-quality validator to the Python analyzer
//! (advanced task #4, PyO3 route). Built with maturin into a module named
//! `aq_validator`:
//!
//! ```python
//! import aq_validator
//! code = aq_validator.validate("pm25", 14.0, "µg/m³", 55.75, 37.6)  # 0 == valid
//! aq_validator.reason_message(code)                                  # "valid"
//! mask = aq_validator.validate_batch(params, values, units, lats, lons)
//! ```

use aq_validator_core::{validate as core_validate, Reason, Validity};
use pyo3::prelude::*;

/// Validate one measurement; returns 0 when valid, else the reason code (1..=5).
#[pyfunction]
#[pyo3(signature = (parameter, value, unit="", latitude=0.0, longitude=0.0))]
fn validate(parameter: &str, value: f64, unit: &str, latitude: f64, longitude: f64) -> i32 {
    match core_validate(parameter, value, unit, latitude, longitude) {
        Validity::Valid => 0,
        Validity::Invalid(r) => r as i32,
    }
}

/// Human-readable message for a reason code.
#[pyfunction]
fn reason_message(code: i32) -> &'static str {
    match code {
        0 => "valid",
        1 => Reason::UnknownParameter.message(),
        2 => Reason::NonFinite.message(),
        3 => Reason::OutOfRange.message(),
        4 => Reason::BadUnit.message(),
        5 => Reason::BadCoordinates.message(),
        _ => "unknown reason",
    }
}

/// Vectorised validation: returns one reason code per input row. Lengths must
/// match `parameters`; missing unit/coords default to "" / 0.0.
#[pyfunction]
#[pyo3(signature = (parameters, values, units=None, latitudes=None, longitudes=None))]
fn validate_batch(
    parameters: Vec<String>,
    values: Vec<f64>,
    units: Option<Vec<String>>,
    latitudes: Option<Vec<f64>>,
    longitudes: Option<Vec<f64>>,
) -> PyResult<Vec<i32>> {
    let n = parameters.len();
    if values.len() != n {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "parameters and values must have equal length",
        ));
    }
    let units = units.unwrap_or_default();
    let lats = latitudes.unwrap_or_default();
    let lons = longitudes.unwrap_or_default();

    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let unit = units.get(i).map(String::as_str).unwrap_or("");
        let lat = lats.get(i).copied().unwrap_or(0.0);
        let lon = lons.get(i).copied().unwrap_or(0.0);
        out.push(match core_validate(&parameters[i], values[i], unit, lat, lon) {
            Validity::Valid => 0,
            Validity::Invalid(r) => r as i32,
        });
    }
    Ok(out)
}

#[pymodule]
fn aq_validator(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(validate, m)?)?;
    m.add_function(wrap_pyfunction!(reason_message, m)?)?;
    m.add_function(wrap_pyfunction!(validate_batch, m)?)?;
    Ok(())
}
