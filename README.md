# Odradek

Odradek is a Kafka SLO metrics exporter. It runs controlled producer and consumer pairs against configured Kafka topics, measures end-to-end latency at multiple points in the message lifecycle, and exposes the results as Prometheus metrics. It also scrapes live topic configuration from the Kafka AdminClient and publishes it as a gauge for topology and configuration visibility.

## Architecture

Odradek is structured around three observer types, each implemented as a Stuart Sierra Component, and an orchestrator that manages their lifecycle and drives their execution.

**Producer observer** — for each configured cluster, creates a `KafkaProducer` and sends a fixed number of messages per rate interval. Each message carries a `com.franz.odradek/produced-at` header with the send timestamp in milliseconds. Production ack latency and error counts are recorded at send time. The rate is driven by a ticker in the orchestrator: a `go-loop` fires into a trigger channel every `rate-interval-ms`, and the producer uses `async/thread` to run the blocking `.send().get()` calls off the core.async thread pool.

**Consumer observer** — for each configured cluster, creates a `KafkaConsumer` and runs a blocking poll loop on a JVM `future`. On startup it seeks all assigned partitions to the end, so only messages produced after startup are measured. On each batch it records fetch latency and message age before committing, then records full end-to-end latency (produce to post-commit) after the synchronous commit. The consumer is self-driven and does not use a trigger channel.

**Topic-info observer** — creates a `KafkaAdminClient` for each configured cluster and periodically scrapes topic descriptions and configs for all topics referenced by producer and consumer observers in that cluster. Topic metadata is published as a gauge metric with label values encoding the full topic configuration. This observer is ticker-driven at `scrape-interval-ms`.

The **orchestrator** starts all observers on system start, runs two separate tickers (one for producers at `rate-interval-ms`, one for topic-info at `scrape-interval-ms`), and coordinates shutdown: it closes trigger channels before stopping components so that blocked `go-loops` exit cleanly without deadlock.

## Metrics reference

The counter and histogram metrics carry these labels: `cluster_name`, `observer`, `topic`, `message_size_kb`, `configured_rate_interval`.

| Metric name | Type | Description |
|---|---|---|
| `kafka_odradek_messages_produced_total` | Counter | Total messages successfully produced |
| `kafka_odradek_messages_production_error_total` | Counter | Total production errors |
| `kafka_odradek_messages_fetched_total` | Counter | Total messages successfully fetched |
| `kafka_odradek_messages_fetch_error_total` | Counter | Total fetch errors |
| `kafka_odradek_messages_production_latency_ms` | Histogram | Time from send to producer ack (ms) |
| `kafka_odradek_messages_fetch_latency_ms` | Histogram | Time from produce to fetch, before commit (ms) |
| `kafka_odradek_e2e_message_age_ms` | Histogram | Message age at fetch time, measured from produced-at header (ms) |
| `kafka_odradek_full_e2e_ms` | Histogram | Time from produce to post-commit (ms) |
| `kafka_odradek_topic_config` | Gauge | Effective topic configuration; always `1.0`, labels carry the configuration values |

The `kafka_odradek_topic_config` gauge uses a separate label set: `cluster_name`, `topic`, `partitions`, `replication_factor`, `partitions_replicas_broker_ids`, `partitions_isr_broker_ids`, `partitions_leader_broker_ids`, `partitions_replicas_broker_racks`, `min_insync_replicas`, `retention_ms`, `retention_bytes`, `cleanup_policy`, `max_message_bytes`, `compression_type`.

Error counters (`production_error_total`, `fetch_error_total`) are initialized to `0` at startup for each configured observer and cluster. This ensures Grafana shows `0` instead of "no data" when no errors have occurred.

Histogram buckets (ms): 5, 10, 15, 25, 30, 40, 50, 80, 100, 250, 300, 500, 800, 1000, 1500, 2000, 3000, 4000, 5000, 10000, 15000, 20000, 25000, 30000.

## Configuration

Odradek reads its configuration from a JSON file. By default it loads `config.json` from the classpath (`resources/config.json`). Set the `CONFIG_PATH` environment variable to read from a filesystem path instead (required in production/Docker deployments).

### Top-level structure

```json
{
  "server": {
    "port": 8082
  },
  "orchestrator-config": {
    "rate-interval-ms": 100,
    "scrape-interval-ms": 30000
  },
  "kafka_clusters": {
    "<cluster-name>": {
      "bootstrap-url": "<host:port>"
    }
  },
  "observers": [ ... ]
}
```

`rate-interval-ms` controls how often producer observers fire. `scrape-interval-ms` controls how often topic-info observers run.

### Observer types

All observers share these required fields:

| Field | Type | Description |
|---|---|---|
| `name` | string | Unique name for this observer. Used to derive the consumer group ID. |
| `clusters` | array of strings | Cluster names from `kafka_clusters` to run against. |
| `observer-type` | string | One of `producer`, `consumer`, `topic-info`. |

**producer** — additional required fields:

| Field | Type | Description |
|---|---|---|
| `topic` | string | Topic to produce to. |
| `volume-config.parallelism` | int | Number of producer instances per cluster. |
| `volume-config.messages-per-interval` | int | Messages sent per rate interval. |
| `volume-config.message-size-kb` | int | Payload size in KB. |
| `producer-config` | object | Kafka producer properties merged on top of defaults. |
| `custom-labels` | object | Optional string key/value pairs attached to metrics labels. |

**consumer** — additional required fields:

| Field | Type | Description |
|---|---|---|
| `topic` | string | Topic to consume from. |
| `consumer-config` | object | Kafka consumer properties merged on top of defaults. |
| `custom-labels` | object | Optional string key/value pairs attached to metrics labels. |

The consumer group ID is derived as `ODRADEK-<OBSERVER-NAME-UPPERCASED>`. Override by setting `"group.id"` in `consumer-config`.

**topic-info** — no additional required fields. The observer automatically discovers which topics to scrape by reading the `topic` field of all other observers that reference the same clusters.

### Minimal example for each observer type

```json
{
  "server": { "port": 8082 },
  "orchestrator-config": {
    "rate-interval-ms": 100,
    "scrape-interval-ms": 30000
  },
  "kafka_clusters": {
    "local-1": { "bootstrap-url": "localhost:9092" }
  },
  "observers": [
    {
      "name": "topic-config",
      "clusters": ["local-1"],
      "observer-type": "topic-info"
    },
    {
      "name": "small-messages-producer",
      "clusters": ["local-1"],
      "topic": "MY-TOPIC",
      "observer-type": "producer",
      "volume-config": {
        "parallelism": 1,
        "messages-per-interval": 10,
        "message-size-kb": 10
      },
      "producer-config": {
        "acks": "all",
        "linger.ms": "0",
        "retries": "3",
        "delivery.timeout.ms": "5000",
        "request.timeout.ms": "1500"
      }
    },
    {
      "name": "small-messages-consumer",
      "clusters": ["local-1"],
      "topic": "MY-TOPIC",
      "observer-type": "consumer",
      "consumer-config": {
        "max.poll.interval.ms": "30000",
        "heartbeat.interval.ms": "3000",
        "session.timeout.ms": "30000"
      }
    }
  ]
}
```

### CONFIG_PATH

When `CONFIG_PATH` is set to a non-empty value, Odradek reads configuration from that filesystem path. When unset or empty, it falls back to the classpath resource `config.json`.

```
CONFIG_PATH=/etc/odradek/config.json
```

## Running locally

Prerequisites: Docker, Java 21+, [Leiningen](https://leiningen.org/)

```bash
# Start Kafka (and Prometheus + Grafana if configured in docker-compose)
make run-deps

# Run the application
lein run
```

The application reads `resources/config.json` from the classpath by default.

- Metrics: http://localhost:8082/metrics
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (admin / admin)

Grafana is provisioned automatically with a dashboard for Odradek metrics.

To stop dependencies:

```bash
make stop-deps
```

## Running with Docker

Build the image:

```bash
docker build -t odradek .
```

Run with a config file mounted from the host:

```bash
docker run \
  -p 8082:8082 \
  -v /path/to/config.json:/etc/odradek/config.json:ro \
  -e CONFIG_PATH=/etc/odradek/config.json \
  odradek
```

The Dockerfile sets `ENV CONFIG_PATH=""` as its default so the classpath fallback is used when `CONFIG_PATH` is not provided. In Kubernetes, mount the config as a ConfigMap volume and set `CONFIG_PATH` to the mounted path.

The image runs as a non-root user (`odradek`, uid 1001) and exposes port 8082.

## Operational endpoints

All operational endpoints return JSON.

| Method | Path | Response |
|---|---|---|
| `GET` | `/ops/liveness` | Always `200 {"status": "alive"}` |
| `GET` | `/ops/readiness` | `200 {"status": "ready"}` if at least one observer reports `:running`; `503 {"status": "not ready"}` otherwise |
| `GET` | `/ops/health` | `200` if no observer is `:stopped` and none has been in backoff for more than 5 minutes; `503` with a checks breakdown otherwise |
| `GET` | `/ops/config/dump` | `200` with the effective parsed config; values whose keys match `password`, `secret`, `token`, or `api-key` are replaced with `"***REDACTED***"` |

The `/ops/health` response shape when unhealthy:

```json
{
  "status": "unhealthy",
  "checks": {
    "all-loops-active": false,
    "no-prolonged-backoff": true
  }
}
```

## Prometheus scraping

Metrics are exposed at `GET /metrics` in Prometheus text format (`text/plain; version=0.0.4; charset=utf-8`). Point your Prometheus scrape config at this endpoint. The scrape interval should be shorter than `rate-interval-ms` to avoid missing increments; 15s is a reasonable default for most configurations.

```yaml
scrape_configs:
  - job_name: odradek
    static_configs:
      - targets: ['<host>:8082']
```

## CI/CD

The GitHub Actions pipeline (`.github/workflows/odradek.yml`) runs on every push to `main` and on every pull request targeting `main`. It also runs on version tags (`v*.*.*`).

The pipeline has three jobs that run in sequence:

1. **unit-tests** — runs `lein test :odradek.unit` with no external dependencies.
2. **integration-tests** — runs `lein test :odradek.integration` with a real Kafka 3.7.0 broker started as a GitHub Actions service container.
3. **build-and-push** — builds the Docker image and pushes it to `joseronierison/odradek` on Docker Hub. This job only runs on `push` events (not pull requests). Images are tagged with the short commit SHA (`sha-<sha>`), `latest` (on `main`), and full/major/minor semver on version tags.

Required repository secrets for the build-and-push job:

| Secret | Description |
|---|---|
| `DOCKERHUB_USERNAME` | Docker Hub username |
| `DOCKERHUB_TOKEN` | Docker Hub access token |

## Running tests

Unit tests have no external dependencies:

```bash
lein test :odradek.unit
# or
make unit
```

Integration tests require a running Kafka broker on `localhost:9092`:

```bash
make run-deps          # starts Kafka via docker-compose
lein test :odradek.integration
# or
make integration       # runs run-deps then the tests
```

To run both suites:

```bash
make test
```

Integration tests use [state-flow](https://github.com/nubank/state-flow) and [matcher-combinators](https://github.com/nubank/matcher-combinators) and spin up a full Component system against the real broker.
