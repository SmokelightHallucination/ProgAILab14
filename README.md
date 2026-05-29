лабораторная работа №14 студент группы 221131 Саранчук Егор выбранный вариант повышенная сложность 20
# Мониторинг качества воздуха — конвейер обработки данных (вариант 20)

Лабораторная работа №14, **повышенный уровень**. Предметная область —
**мониторинг качества воздуха**, источник данных — **OpenAQ API** (с офлайн-
эмуляцией для разработки и CI).

Конвейер: **Go-сборщик** собирает измерения концентраций загрязнителей
(PM2.5, PM10, NO₂, O₃, SO₂, CO) по станциям мира, валидирует их
**Rust-библиотекой**, агрегирует **оконно** и раздаёт потребителям;
**Python-анализатор** обрабатывает данные через **Polars / DuckDB / Parquet**
и строит визуализации; **Streamlit-дашборд** показывает статистику в реальном
времени. Координация экземпляров сборщика — через **etcd**, потоковая передача —
через **Kafka**, нулевое копирование — через **Apache Arrow Flight**.

## Архитектура

```
                         ┌──────────────┐
                         │     etcd     │  координация шардов (rendezvous-хэш)
                         └──────┬───────┘
              регистрация/watch │
        ┌───────────────────────┼───────────────────────┐
        ▼                       ▼                         
┌───────────────┐       ┌───────────────┐                
│  collector-1  │       │  collector-2  │   Go, своя доля станций каждому
│  (Go)         │       │  (Go)         │                
│               │       │               │                
│ source ──▶ validate(Rust/cgo) ──▶ tumbling window ──▶ aggregate (AVG/MIN/MAX/AQI)
└──────┬────────┘       └──────┬────────┘                
       │  fan-out sinks        │                          
       ├───────────────┬───────┴────────────┐            
       ▼               ▼                     ▼            
┌────────────┐  ┌──────────────┐     ┌──────────────┐    
│   Kafka    │  │ Arrow Flight │     │   Parquet    │    
│   topic    │  │  (gRPC 8815) │     │   (файл)     │    
└─────┬──────┘  └──────┬───────┘     └──────┬───────┘    
      │                │                    │            
      ▼                ▼                    ▼            
┌───────────────────────────────────────────────────────┐
│                   Python (Polars / DuckDB)             │
│  • stream.py   — скользящее окно 5 мин (Kafka)         │
│  • flight_client.py — zero-copy приём (Arrow)         │
│  • batch.py    — анализ Parquet + SQL                  │
│  • viz.py / viz_static.py — графики (HTML + PNG)       │
│  • validation.py — та же Rust-библиотека через PyO3    │
└───────────────────────┬───────────────────────────────┘
                        ▼
              ┌────────────────────┐
              │ Streamlit dashboard│  карта AQI, ряды, категории — live
              │  (port 8501)       │
              └────────────────────┘
```

Подробное описание компонентов — в [docs/architecture.md](docs/architecture.md).

## Соответствие заданиям повышенного уровня

| # | Задание | Реализация |
|---|---------|-----------|
| 1 | Распределённый сборщик (etcd) | [`collector/internal/coord`](collector/internal/coord) — rendezvous-хэширование станций, lease + watch |
| 2 | Оконная агрегация в Go | [`collector/internal/window`](collector/internal/window) — tumbling window, AVG/MIN/MAX/SUM/COUNT + AQI |
| 3 | Apache Arrow | [`collector/internal/sink/flight.go`](collector/internal/sink/flight.go) сервер + [`analyzer/.../flight_client.py`](analyzer/aqanalyzer/flight_client.py) клиент |
| 4 | Rust-валидация | [`validator/`](validator) — ядро + cgo (Go) + PyO3 (Python) |
| 5 | Kubernetes + HPA | [`k8s/`](k8s) — Deployment, Service, HPA (CPU / queue-length) |
| — | Go vs Python | [`collector_py/`](collector_py) + отчёт [`docs/benchmark.md`](docs/benchmark.md) |
| — | Потоки (Kafka) | Kafka-sink в Go + [`analyzer/.../stream.py`](analyzer/aqanalyzer/stream.py) (скользящее окно 5 мин) |
| 6 | Веб-дашборд (real-time) | [`dashboard/app.py`](dashboard/app.py) — Streamlit |

## Быстрый старт (Docker Compose)

Поднимает etcd, Kafka, два сборщика (с шардированием), анализатор и дашборд:

```bash
docker compose up --build
```

- Дашборд: http://localhost:8501
- Метрики сборщика: http://localhost:9101/metrics (и :9102 для collector-2)
- Arrow Flight: `grpc://localhost:8815`

Использовать реальный OpenAQ API вместо эмуляции:

```bash
export SOURCE=openaq
export OPENAQ_API_KEY=<ваш ключ>   # получить на https://openaq.org
docker compose up --build
```

Остановить и удалить тома:

```bash
docker compose down -v
```

## Запуск без Docker (локально)

### 1. Сгенерировать демо-данные (Parquet) и проанализировать

```bash
# Go: сгенерировать выборку агрегатов (синтетический источник, без сервисов)
cd collector
go run ./cmd/gensample -out ../data/aggregates.parquet -windows 30

# Python: окружение анализатора
cd ../analyzer
python -m venv .venv && . .venv/Scripts/activate    # Linux/Mac: source .venv/bin/activate
pip install -r requirements.txt

# (необязательно) собрать Rust-валидатор как PyO3-модуль
pip install maturin
maturin develop --release --manifest-path ../validator/py/Cargo.toml

# Анализ: Polars + DuckDB + графики в docs/figures/
PYTHONPATH=. python -m scripts.analyze_batch ../data/aggregates.parquet
```

### 2. Распределённый сбор + потоки (нужны etcd и Kafka)

```bash
docker compose up -d etcd kafka          # только инфраструктура
cd collector
# два сборщика в разных терминалах — поделят станции через etcd
INSTANCE_ID=c1 ETCD_ENDPOINTS=localhost:2379 KAFKA_ENABLED=true FLIGHT_ENABLED=true go run .
INSTANCE_ID=c2 ETCD_ENDPOINTS=localhost:2379 KAFKA_ENABLED=true PARQUET_ENABLED=false go run .

# Python: чтение из Kafka со скользящим окном 5 минут
cd ../analyzer && PYTHONPATH=. python -m scripts.consume_stream
# Приём через Arrow Flight (zero-copy)
PYTHONPATH=. python -m scripts.pull_flight
```

### 3. Дашборд реального времени

```bash
cd dashboard && pip install -r requirements.txt
streamlit run app.py        # http://localhost:8501
```

### 4. Бенчмарк Go vs Python

```bash
cd collector && go build -o collector.exe .   # Linux/Mac: go build -o collector .
cd ../collector_py && pip install -r requirements.txt
PYTHONPATH=. python -m scripts.benchmark --duration 20
# → docs/benchmark.md + docs/figures/benchmark.png
```

## Развёртывание в Kubernetes (minikube/k3s)

```bash
minikube start
minikube addons enable metrics-server          # нужно для HPA по CPU

# собрать образы в docker-демоне minikube
eval $(minikube docker-env)
docker build -t airquality/collector:latest -f collector/Dockerfile .
docker build -t airquality/analyzer:latest  -f analyzer/Dockerfile .
docker build -t airquality/dashboard:latest -f dashboard/Dockerfile .

kubectl apply -f k8s/
kubectl -n airquality get hpa -w               # наблюдать автоскейл
minikube service -n airquality dashboard       # открыть дашборд
```

При росте нагрузки HPA увеличивает число реплик сборщика; etcd перераспределяет
станции между ними автоматически (rendezvous-хэш).

## Конфигурация (переменные окружения сборщика)

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `SOURCE` | `synthetic` | `synthetic` или `openaq` |
| `OPENAQ_API_KEY` | — | ключ OpenAQ при `SOURCE=openaq` |
| `ETCD_ENDPOINTS` | — | список etcd (пусто → standalone) |
| `WINDOW_SIZE` | `10s` | длина tumbling-окна |
| `POLL_INTERVAL` | `2s` | период опроса (`0s` → максимальная скорость) |
| `KAFKA_ENABLED` / `KAFKA_BROKERS` / `KAFKA_TOPIC` | `false` / `localhost:9092` / `air-quality.aggregates` | поток Kafka |
| `PARQUET_ENABLED` / `PARQUET_PATH` | `true` / `data/aggregates.parquet` | запись Parquet |
| `FLIGHT_ENABLED` / `FLIGHT_ADDR` | `false` / `0.0.0.0:8815` | Arrow Flight сервер |
| `METRICS_ADDR` | `0.0.0.0:9100` | `/metrics`, `/healthz` |

## Тесты

```bash
cd collector && go test ./...        # окно, Parquet round-trip
cd validator && cargo test           # правила валидации
```

## Структура репозитория

```
collector/      Go-сборщик (source, coord/etcd, window, sink/{kafka,parquet,flight}, validate)
  cmd/gensample/  генератор демо-Parquet без внешних сервисов
collector_py/   Python (asyncio) сборщик + бенчмарк Go vs Python
validator/      Rust: core + ffi (cgo) + py (PyO3)
analyzer/       Python-анализ: Polars, DuckDB, Arrow Flight, Kafka, визуализации
dashboard/      Streamlit real-time дашборд
k8s/            манифесты Kubernetes + HPA
docs/           архитектура, отчёт о производительности, графики
```

## Стек

Go 1.24 · Rust 1.94 (cgo + PyO3) · Python 3.12 (Polars, DuckDB, PyArrow, Plotly,
Streamlit) · etcd · Kafka (KRaft) · Apache Arrow Flight · Docker Compose ·
Kubernetes (HPA).
