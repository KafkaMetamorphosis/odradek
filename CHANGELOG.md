# Changelog

All notable changes to the Odradek project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `prune-deps` Makefile target: stops local containers and deletes named volumes (`docker-compose down -v`) for a clean-slate reset
- `docker-compose.test.yaml` (renamed from `docker-compose.yaml`): single-broker Kafka on port 9092, used exclusively by the `integration` Makefile target
- `docker-compose.local.yml`: local dev setup with 3 independent Kafka clusters (kafka-1:19092, kafka-2:9094, kafka-3:9096), Prometheus, and Grafana
- Grafana dashboard "SLO Overview per Cluster": state timeline SLO compliance per cluster (local-1→10KB, local-2→1MB, local-3→9MB observers)
- Cluster filter variable added to all Grafana dashboards

### Fixed
- Grafana dashboards: removed all references to non-existent metrics (`kafka_odradek_topic_partitions`, `kafka_odradek_topic_replication_factor`, `kafka_odradek_topic_min_isr`) and replaced them with queries on `kafka_odradek_topic_config`
- Grafana state timeline panels: replaced boolean PASS/FAIL (`> bool N`) with SLO compliance percentage (0–100%) using `rate(bucket{le=...}) / rate(bucket{le="+Inf"}) * 100`
- Kafka broker logs now stored in Docker named volumes (kafka-1-data, kafka-2-data, kafka-3-data) to prevent disk exhaustion in /tmp
- Grafana state timeline panels: replaced bucket-ratio PromQL with histogram_quantile <= bool pattern to fix "Data does not have a time field" error caused by le label format mismatch (integer vs float)

### Changed
- Kafka broker defaults set to 5-minute retention (`retention.ms=300000`) and 50 MB cap (`retention.bytes=52428800`) in both `docker-compose.local.yml` and `docker-compose.test.yaml`
- Seed script topic retention corrected from 15 minutes (`900000 ms`) to 5 minutes (`300000 ms`) to match broker defaults
- `Makefile`: `run-deps`/`stop-deps` use `docker-compose.local.yml`; `integration` target now uses `docker-compose.test.yaml` (renamed)
- `docker-compose.local.yml`: kafka-1 host port changed from 9092 to 19092 to avoid collision with the test broker on port 9092
- `config.json`: local-1 bootstrap-url updated to `localhost:19092`; observers redistributed across 3 clusters; dead `observe-configs` field removed; local-2/local-3 clusters added
- `seed.clj`: seeds topics on all 3 clusters
- Grafana SLO Dashboard: 4 state timeline panels added for SLO compliance; `cluster_name` filter variable added; all panel queries scoped to cluster
- Grafana Topic Overview: topic variable changed to single-value; stale `kafka_odradek_topic_config_retention_ms` and `kafka_odradek_topic_config_retention_bytes` metric references fixed
- Grafana Topic Scrape Performance: cluster variable changed to multi-value with includeAll; panel queries updated to use regex matcher
- CI/CD: Docker images are now built and pushed only on semver git tags, not on every commit to main.
- Docker image tags follow semver convention (`1.2.3`, `1.2`, `1`, `latest`).

[Unreleased]: https://github.com/KafkaMetamorphosis/odradek/compare/HEAD...HEAD
