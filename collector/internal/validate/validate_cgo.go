//go:build rustvalidate

package validate

/*
#cgo CFLAGS: -I${SRCDIR}/../../../validator/ffi/include
#cgo LDFLAGS: -L${SRCDIR}/../../../validator/target/release -laq_validator_ffi
#cgo linux LDFLAGS: -lm
#include <stdlib.h>
#include "aq_validator.h"
*/
import "C"

import (
	"unsafe"

	"airquality/collector/internal/model"
)

const backend = "rust-cgo"

// check calls the Rust validator (validator/ffi) over cgo. Build the static
// library first: `cargo build -p aq_validator_ffi --release`, then build the
// collector with `-tags rustvalidate`.
func check(m model.Measurement) Result {
	cParam := C.CString(m.Parameter)
	cUnit := C.CString(m.Unit)
	defer C.free(unsafe.Pointer(cParam))
	defer C.free(unsafe.Pointer(cUnit))

	code := int(C.aq_validate(cParam, C.double(m.Value), cUnit,
		C.double(m.Latitude), C.double(m.Longitude)))
	if code == 0 {
		return Result{OK: true, Code: 0, Reason: "valid"}
	}
	msg := C.GoString(C.aq_reason_message(C.int(code)))
	return Result{OK: false, Code: code, Reason: msg}
}
