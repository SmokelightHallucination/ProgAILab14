# Python analyzer image. Build context = repository root so the Rust validator
# can be compiled into the PyO3 module (advanced task #4, Python route).
#
#   stage 1: build the aq_validator wheel with maturin
#   stage 2: slim runtime with the analyzer + wheel installed

# ---- stage 1: build the PyO3 validator wheel --------------------------------
FROM rust:1.94-bookworm AS wheel
RUN apt-get update && apt-get install -y python3 python3-pip python3-venv && rm -rf /var/lib/apt/lists/*
RUN pip3 install --break-system-packages maturin
WORKDIR /src
COPY validator/ ./validator/
RUN cd validator/py && maturin build --release --out /wheels

# ---- stage 2: analyzer runtime ---------------------------------------------
FROM python:3.12-slim
WORKDIR /app
COPY analyzer/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
# Install the Rust validator wheel (PyO3). If absent the analyzer falls back to
# its pure-Python validator automatically.
COPY --from=wheel /wheels/ /wheels/
RUN pip install --no-cache-dir /wheels/*.whl || echo "validator wheel not installed; using python fallback"
COPY analyzer/ ./
ENV PYTHONUNBUFFERED=1 PYTHONUTF8=1 PYTHONPATH=/app
ENTRYPOINT ["python", "-m"]
CMD ["scripts.consume_stream"]
