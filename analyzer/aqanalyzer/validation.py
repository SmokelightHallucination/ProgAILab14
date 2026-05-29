"""Validation bridge to the Rust library (advanced task #4, PyO3 route).

If the compiled ``aq_validator`` extension is importable it is used; otherwise a
pure-Python mirror of the same rules runs, so the analyzer works even when the
Rust toolchain is unavailable. ``validate_frame`` adds ``valid``/``reason``
columns to a Polars frame of measurements.
"""

from __future__ import annotations

import math

import polars as pl

_SPECS = {
    "pm25": ("µg/m³", 0.0, 1000.0),
    "pm10": ("µg/m³", 0.0, 2000.0),
    "no2": ("µg/m³", 0.0, 3000.0),
    "o3": ("µg/m³", 0.0, 1000.0),
    "so2": ("µg/m³", 0.0, 3000.0),
    "co": ("mg/m³", 0.0, 100.0),
}

_MESSAGES = {
    0: "valid",
    1: "unknown pollutant parameter",
    2: "value is NaN, infinite or negative",
    3: "value outside plausible range for pollutant",
    4: "unit does not match expected unit",
    5: "latitude/longitude outside WGS84 bounds",
}

try:
    import aq_validator as _rust  # type: ignore

    BACKEND = "rust-pyo3"
except ImportError:  # pragma: no cover - depends on build environment
    _rust = None
    BACKEND = "python-native"


def _py_validate(parameter: str, value: float, unit: str, lat: float, lon: float) -> int:
    spec = _SPECS.get(parameter)
    if spec is None:
        return 1
    expected_unit, lo, hi = spec
    if value is None or math.isnan(value) or math.isinf(value) or value < 0:
        return 2
    if value < lo or value > hi:
        return 3
    if unit and unit != expected_unit:
        return 4
    if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
        return 5
    return 0


def validate(parameter: str, value: float, unit: str = "", lat: float = 0.0, lon: float = 0.0) -> int:
    """Validate one measurement, returning a reason code (0 == valid)."""
    if _rust is not None:
        return _rust.validate(parameter, value, unit, lat, lon)
    return _py_validate(parameter, value, unit, lat, lon)


def reason_message(code: int) -> str:
    if _rust is not None:
        return _rust.reason_message(code)
    return _MESSAGES.get(code, "unknown reason")


def validate_frame(
    df: pl.DataFrame,
    *,
    parameter="parameter",
    value="avg",
    unit="unit",
    lat="latitude",
    lon="longitude",
) -> pl.DataFrame:
    """Return ``df`` with integer ``code``, boolean ``valid`` and ``reason``."""
    params = df[parameter].to_list()
    values = df[value].to_list()
    units = df[unit].to_list() if unit in df.columns else [""] * len(df)
    lats = df[lat].to_list() if lat in df.columns else [0.0] * len(df)
    lons = df[lon].to_list() if lon in df.columns else [0.0] * len(df)

    if _rust is not None and hasattr(_rust, "validate_batch"):
        codes = _rust.validate_batch(params, values, units, lats, lons)
    else:
        codes = [
            validate(p, v, u, la, lo)
            for p, v, u, la, lo in zip(params, values, units, lats, lons)
        ]

    return df.with_columns(
        pl.Series("code", codes, dtype=pl.Int32),
        pl.Series("valid", [c == 0 for c in codes]),
        pl.Series("reason", [reason_message(c) for c in codes]),
    )
