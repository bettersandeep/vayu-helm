# Changelog

All notable vayu-fork changes are documented here. Format based on
[Keep a Changelog](https://keepachangelog.com/); versions follow `vayu-<X.Y.Z>+olake<base>`.

## [vayu-0.2.0+olake0.3.10] - 2026-07-22

### Added
- Fusion Prometheus metrics: optional Amoro prometheus-exporter plugin (values-gated, default off),
  a fusion-metrics Service, and a ServiceMonitor. Surfaces amoro_optimizer_group_optimizer_instances
  and amoro_optimizer_group_threads for dead-optimizer alerting.
- Fusion AMS maintenance executors: expire-snapshots, clean-orphan-files, and
  clean-dangling-delete-files (values-gated, default on) so per-table table-expire/clean-orphan-file
  properties actually reclaim object storage.

### Changed
- Chart version bumped to `0.2.0`.

## [vayu-0.1.0+olake0.3.10] - 2026-07-19

First tagged release of the vayu-helm fork (based on upstream olake-helm `v0.3.10`).

### Added
- Worker Sentry integration (init, temporal interceptor, classified failure capture, SyncFailed event).
- Prometheus metrics: job runs, sync duration (histogram), records synced; ServiceMonitor selector fix.
- Executor error propagation and configurable job-pod resources via env variables.
- Helm Sentry DSN/release injection and configurable container registry base.
- `vayu-release.yaml` platform manifest pinning the validated component set.

### Changed
- Chart version reset to the vayu release line (`0.1.0`).
