# Build context = repository root (see docker-compose.yml), so both the Go
# collector and the Rust validator are available for the cgo link.
#
#   stage 1: build the Rust validator static library
#   stage 2: build the Go collector with cgo, linking the Rust lib (-tags rustvalidate)
#   stage 3: minimal Debian runtime

# ---- stage 1: Rust validator ------------------------------------------------
FROM rust:1.94-bookworm AS rust
WORKDIR /src
COPY validator/ ./validator/
RUN cargo build --manifest-path validator/Cargo.toml -p aq_validator_ffi --release

# ---- stage 2: Go collector (cgo + Rust) ------------------------------------
FROM golang:1.24-bookworm AS go
WORKDIR /src
# Bring the Rust artifacts so the cgo LDFLAGS path resolves identically to local.
COPY --from=rust /src/validator/target/release/libaq_validator_ffi.a ./validator/target/release/
COPY --from=rust /src/validator/ffi/include/ ./validator/ffi/include/
COPY collector/go.mod collector/go.sum ./collector/
RUN cd collector && go mod download
COPY collector/ ./collector/
RUN cd collector && CGO_ENABLED=1 go build -tags rustvalidate -ldflags "-s -w" -o /out/collector .

# ---- stage 3: runtime -------------------------------------------------------
FROM debian:bookworm-slim
RUN useradd -m -u 10001 aq
COPY --from=go /out/collector /usr/local/bin/collector
USER aq
EXPOSE 9100 8815
ENTRYPOINT ["/usr/local/bin/collector"]
