# Odradek

Kafka SLO metrics exporter. Runs controlled producer/consumer pairs against Kafka topics and exposes end-to-end latency and error rate metrics in Prometheus format.

## How it works

For each configured observer × cluster pair, Odradek:

1. **Produces** synthetic messages at a fixed rate, embedding a timestamp header in each record.
2. **Consumes** from the same topic (seeking to the end on startup), decoding the timestamp to measure latency at fetch and after commit.
3. **Exposes** all metrics at `GET /metrics` in Prometheus text format.

## Metrics

All metrics carry these labels: `cluster_name`, `observer`, `topic`, `message_size_kb`, `configured_rate_interval`.

| Metric | Type | Description |
|---|---|---|
| `kafka_odradek_messages_produced_total` | Counter | Messages successfully produced |
| `kafka_odradek_messages_production_error_total` | Counter | Production errors |
| `kafka_odradek_messages_fetched_total` | Counter | Messages successfully fetched |
| `kafka_odradek_messages_fetch_error_total` | Counter | Fetch errors |
| `kafka_odradek_messages_production_latency_ms` | Histogram | Producer ack latency |
| `kafka_odradek_messages_fetch_latency_ms` | Histogram | Time from produce to fetch (pre-commit) |
| `kafka_odradek_e2e_message_age_ms` | Histogram | Message age at fetch (pre-commit) |
| `kafka_odradek_full_e2e_ms` | Histogram | Full produce-to-commit latency |

## Running locally

**Prerequisites:** Docker, Java 21+, [Leiningen](https://leiningen.org/)

```bash
# Start Kafka, Prometheus, and Grafana
make run-deps

# Run the application
make run
```

- Metrics: http://localhost:8082/metrics
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (admin / admin)

Grafana is provisioned automatically with a dashboard for Odradek metrics.

## Configuration

Config is read from `resources/config.edn` using [Aero](https://github.com/juxt/aero) profiles.

| Env var | Default | Description |
|---|---|---|
| `PORT` | `8082` | HTTP server port |
| `KAFKA_BOOTSTRAP_URL` | `localhost:9092` | Kafka bootstrap address |
| `RATE_INTERVAL_MS` | `100` | Interval between produce batches (ms) |

### Observer config

Each observer defines what to produce and consume:

```edn
{:name                "my-observer"   ; used to derive consumer group.id
 :clusters            ["local-1"]     ; must match a key in :kafka_clusters
 :topic               "MY-TOPIC"
 :parallelism         1               ; producer threads per cluster
 :messages-per-bucket 10             ; messages produced per rate interval
 :message-size-kb     100            ; payload size in KB
 :producer-config     {"acks" "all" "retries" "3"}   ; merged with defaults
 :consumer-config     {}}                             ; merged with defaults
```

Consumer group ID is derived as `ODRADEK-<OBSERVER-NAME-UPPERCASED>`. Override with `"group.id"` in `:consumer-config`.

## HTTP API

| Method | Path | Description |
|---|---|---|
| `GET` | `/metrics` | Prometheus metrics (text/plain) |
| `GET` | `/ops/liveness` | Always 200 — process is alive |
| `GET` | `/ops/readiness` | 200 if any observer is running, 503 otherwise |
| `GET` | `/ops/health` | 200 if all observers healthy, 503 with details otherwise |
| `GET` | `/ops/config/dump` | Effective config (secrets redacted) |

## Testing

```bash
# Unit tests only
make unit

# Integration tests (starts Kafka via docker-compose)
make integration

# Both
make test
```

Integration tests use [state-flow](https://github.com/nubank/state-flow) and spin up a full Component system against a real Kafka broker.

## Architecture

```
core.clj
  └── system.clj (Stuart Sierra Component graph)
        ├── config            — loads config.edn (Aero)
        ├── metrics-registry  — Prometheus registry (8 metrics)
        ├── observer-engine   — producer + consumer threads, rate loop
        ├── api               — Ring routes + middleware
        └── http-server       — Jetty
```

Key design decisions:

- Producer threads use `Thread.` (blocking `.send()/.get()`), not `go` blocks.
- One `core.async` channel per observer × cluster, buffered at `parallelism`. Overflow is drained (not blocked) so the rate loop never stalls.
- Consumer seeks to end on startup — only messages produced after startup are measured.
- Error counters are initialized to `0` at startup so Grafana shows `0` instead of "no data".
