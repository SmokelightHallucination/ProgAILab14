/* C ABI for the Rust air-quality validator (linked into the Go collector via
 * cgo). Implemented in validator/ffi/src/lib.rs. */
#ifndef AQ_VALIDATOR_H
#define AQ_VALIDATOR_H

#ifdef __cplusplus
extern "C" {
#endif

/* Validate one measurement.
 * Returns 0 when valid, otherwise a positive reason code:
 *   1 unknown parameter, 2 non-finite/negative, 3 out of range,
 *   4 bad unit, 5 bad coordinates. */
int aq_validate(const char *parameter, double value, const char *unit,
                double latitude, double longitude);

/* Static, NUL-terminated message for a reason code. Do not free. */
const char *aq_reason_message(int code);

#ifdef __cplusplus
}
#endif

#endif /* AQ_VALIDATOR_H */
