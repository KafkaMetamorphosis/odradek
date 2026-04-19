# Changelog

All notable changes to the Odradek project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `docker-compose.test.yaml` (renamed from `docker-compose.yaml`): single-broker Kafka on port 9092, used exclusively by the `integration` Makefile target
- `docker-compose.local.yml`: local dev setup with 3 independent Kafka clusters (kafka-1:19092, kafka-2:9094, kafka-3:9096), Prometheus, and Grafana
- Grafana dashboard "SLO Overview per Cluster": state timeline SLO compliance per cluster (local-1→10KB, local-2→1MB, local-3→9MB observers)
- Cluster filter variable added to all Grafana dashboards

### Changed
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
