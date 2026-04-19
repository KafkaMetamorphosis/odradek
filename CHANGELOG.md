# Changelog

All notable changes to the Odradek project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- CI/CD: Docker images are now built and pushed only on semver git tags, not on every commit to main.
- Docker image tags follow semver convention (`1.2.3`, `1.2`, `1`, `latest`).
