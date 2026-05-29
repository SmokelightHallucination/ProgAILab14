# Архитектура конвейера

## Предметная модель

Единица данных — измерение концентрации одного загрязнителя на одной станции:

| Поле | Тип | Описание |
|---|---|---|
| `location_id`, `location`, `city`, `country` | string | идентификация станции |
| `latitude`, `longitude` | float64 | координаты (WGS84) |
| `parameter` | string | `pm25`, `pm10`, `no2`, `o3`, `so2`, `co` |
| `value` | float64 | концентрация |
| `unit` | string | `µg/m³` (газы/частицы) или `mg/m³` (CO) |
| `timestamp` | time | момент измерения (UTC) |

После оконной агрегации единицей становится **Aggregate**: одна строка на
`(станция, загрязнитель, окно)` с полями `count/sum/avg/min/max`, временными
границами окна и рассчитанным **US EPA AQI** + категорией здоровья
(`Good … Hazardous`). Расчёт AQI — кусочно-линейная интерполяция по таблицам
EPA, см. [`collector/internal/model/measurement.go`](../collector/internal/model/measurement.go).

## Go-сборщик

Поток внутри одного экземпляра:

```
source.Fetch(assigned_ids) ─▶ validate.Check ─▶ window.Add ─(каждые WINDOW_SIZE)▶ window.Flush
                                   │ invalid                                          │
                                   ▼                                                  ▼
                          metrics.InvalidTotal                              queue ─▶ publisher ─▶ sinks
```

- **Источники** (`internal/source`): `synthetic` — физически правдоподобный
  генератор (суточный цикл + шум + редкие аномалии для валидатора); `openaq` —
  клиент OpenAQ v3 API (`X-API-Key`).
- **Координация** (`internal/coord`): каждый экземпляр кладёт в etcd ключ
  `/aq/collectors/<id>` под lease с keepalive и следит (`Watch`) за множеством
  участников. Владение станцией определяется **rendezvous-хэшированием**
  (highest-random-weight): станция принадлежит участнику с максимальным
  `hash(member|station)`. При входе/выходе экземпляра переназначается лишь
  минимальная доля станций, покрытие остаётся полным без дублей.
- **Оконная агрегация** (`internal/window`): tumbling-окно фиксированной длины;
  на закрытии отдаёт агрегаты и открывает следующее. Передаётся ~`N`× меньше
  данных, чем сырых измерений (где `N` — число опросов за окно).
- **Валидация** (`internal/validate`): по умолчанию чистый Go (mirrors Rust),
  при сборке с `-tags rustvalidate` — вызов Rust-библиотеки через cgo.
- **Синки** (`internal/sink`): Kafka (JSON, ключ = станция), Parquet (Snappy,
  один row-group на окно), Arrow Flight (gRPC-сервер с кольцевым буфером
  последних окон). Раздача параллельна через буферизованную очередь —
  её глубина (`aq_queue_length`) служит сигналом для HPA.
- **Метрики** (`internal/metrics`): Prometheus на `/metrics`, `/healthz`.
- **Graceful shutdown**: по SIGINT/SIGTERM окно дофлашивается, очередь
  дренируется, синки закрываются (Parquet финализируется).

## Rust-валидатор

Воркспейс из трёх крейтов с общим ядром правил:

- `core` — чистая логика: известность параметра, конечность и неотрицательность
  значения, диапазон по загрязнителю, соответствие единицы, границы координат.
- `ffi` — `staticlib` с C-ABI (`aq_validate`, `aq_reason_message`), линкуется в
  Go через cgo.
- `py` — `cdylib` через PyO3, собирается maturin в модуль `aq_validator`
  (функции `validate`, `validate_batch`, `reason_message`).

И Go, и Python имеют встроенный fallback на случай отсутствия скомпилированной
библиотеки, поэтому конвейер работает всегда, а Rust добавляет единый источник
правил и скорость батч-валидации.

## Python-анализатор

- **batch.py** — `pl.read_parquet`, очистка (дедуп + валидация Rust/fallback),
  агрегаты Polars, тот же запрос в DuckDB (`read_parquet`) с замером времени.
- **stream.py** — `KafkaConsumer` + `SlidingWindow` (скользящее окно 5 минут):
  выселение по `window_end`, пересчёт rolling-статистик.
- **flight_client.py** — `pyarrow.flight` DoGet → `pl.from_arrow` (zero-copy),
  плюс счётчики строк/байт для сравнения объёма передачи.
- **viz.py / viz_static.py** — Plotly (интерактивный HTML) и Matplotlib
  (надёжный PNG): временной ряд AQI, распределение по загрязнителям, тепловая
  карта город×загрязнитель, доли категорий AQI.

## Форматы и каналы передачи

| Канал | Формат | Назначение |
|---|---|---|
| Parquet-файл | колоночный, Snappy | батч-анализ (Polars/DuckDB) |
| Kafka-топик | JSON | потоковая обработка реального времени |
| Arrow Flight | Arrow IPC (gRPC) | zero-copy, минимальный объём, сравнение |

## Потоковая модель окон

- **Go, tumbling**: непересекающиеся окна `WINDOW_SIZE` — снижают объём перед
  отправкой.
- **Python, sliding**: скользящее окно 5 минут поверх агрегатов из Kafka — для
  актуальной картины в дашборде и стрим-анализе.
