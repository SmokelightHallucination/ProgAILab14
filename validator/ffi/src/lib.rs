//! C ABI over `aq_validator_core`, linked into the Go collector through cgo.
//! See `include/aq_validator.h` for the matching C declarations.

use std::ffi::{c_char, CStr};

use aq_validator_core::{validate, Validity};

/// Validate one measurement. String arguments are NUL-terminated UTF-8.
///
/// Returns `0` when the reading is valid, otherwise the positive `Reason`
/// discriminant (1..=5). A null/invalid `parameter` pointer returns `1`
/// (unknown parameter).
///
/// # Safety
/// `parameter` and `unit` must be valid NUL-terminated C strings (or null).
#[no_mangle]
pub unsafe extern "C" fn aq_validate(
    parameter: *const c_char,
    value: f64,
    unit: *const c_char,
    latitude: f64,
    longitude: f64,
) -> i32 {
    let parameter = match cstr(parameter) {
        Some(s) => s,
        None => return 1, // UnknownParameter
    };
    let unit = cstr(unit).unwrap_or("");

    match validate(parameter, value, unit, latitude, longitude) {
        Validity::Valid => 0,
        Validity::Invalid(reason) => reason as i32,
    }
}

/// Human-readable message for a reason code (1..=5). Returns a static,
/// NUL-terminated C string; the caller must NOT free it.
#[no_mangle]
pub extern "C" fn aq_reason_message(code: i32) -> *const c_char {
    let msg: &'static CStr = match code {
        0 => c"valid",
        1 => c"unknown pollutant parameter",
        2 => c"value is NaN, infinite or negative",
        3 => c"value outside plausible range for pollutant",
        4 => c"unit does not match expected unit",
        5 => c"latitude/longitude outside WGS84 bounds",
        _ => c"unknown reason",
    };
    msg.as_ptr()
}

unsafe fn cstr<'a>(p: *const c_char) -> Option<&'a str> {
    if p.is_null() {
        return None;
    }
    CStr::from_ptr(p).to_str().ok()
}
