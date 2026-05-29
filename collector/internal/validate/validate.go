// Package validate gates measurements before they enter the aggregation
// window. There are two implementations selected at build time:
//
//   - default (pure Go): mirrors the Rust rules, so the collector builds and
//     runs anywhere without a Rust toolchain.
//   - build tag `rustvalidate`: calls the Rust library (validator/ffi) through
//     cgo — this is the integration required by advanced task #4 and what the
//     Docker image is built with.
//
// Both implementations expose the same Check signature and identical rules.
package validate

import "airquality/collector/internal/model"

// Result is the outcome of validating one measurement.
type Result struct {
	OK     bool
	Code   int    // 0 valid, else reason code matching the Rust enum
	Reason string // human-readable message
}

// Check validates a single measurement.
func Check(m model.Measurement) Result {
	return check(m)
}

// Backend reports which validation implementation is compiled in.
func Backend() string { return backend }
