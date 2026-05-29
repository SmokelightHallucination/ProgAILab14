# Convenience targets for the air-quality pipeline (variant 20).
# On Windows use Git Bash / WSL, or run the underlying commands directly.

.PHONY: help test gensample analyze bench up down rust-test validator-py

help:
	@echo "Targets:"
	@echo "  make test          - Go + Rust unit tests"
	@echo "  make gensample     - generate data/aggregates.parquet (offline)"
	@echo "  make analyze       - run batch analysis + figures"
	@echo "  make bench         - Go vs Python collector benchmark"
	@echo "  make validator-py  - build the PyO3 validator into analyzer/.venv"
	@echo "  make up / down     - docker compose up --build / down -v"

test:
	cd collector && go test ./...
	cd validator && cargo test -p aq_validator_core

gensample:
	cd collector && go run ./cmd/gensample -out ../data/aggregates.parquet -windows 30

analyze:
	cd analyzer && PYTHONPATH=. FIGURES_DIR=../docs/figures python -m scripts.analyze_batch ../data/aggregates.parquet

bench:
	cd collector && go build -o collector .
	cd collector_py && PYTHONPATH=. python -m scripts.benchmark --duration 20

validator-py:
	cd validator/py && maturin develop --release

up:
	docker compose up --build

down:
	docker compose down -v
