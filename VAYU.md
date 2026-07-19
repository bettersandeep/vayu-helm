# Vayu fork

Vayu-helm is a customized fork of [datazip-inc/olake-helm](https://github.com/datazip-inc/olake-helm),
covering the worker and the Helm chart. This document tracks what the fork adds on top of upstream.

**Upstream base:** olake-helm `v0.3.10`

## Custom features

- **Worker Sentry integration** - init, temporal interceptor, classified failure capture, and a
  `SyncFailed` event plumbed through the workflow to the cleanup activity, with credential scrubbing.
- **Prometheus metrics** - `olake_job_runs_total`, `olake_sync_duration_seconds` (histogram), and
  `olake_records_synced_total`, gated on the sync outcome; ServiceMonitor selector fix.
- **Executor & config plumbing** - error propagation from the executor, configurable job-pod
  resources via env variables, configurable container registry base, JVM tuning.
- **Helm wiring** - Sentry DSN/environment injection into ui/worker/fusion, `SENTRY_RELEASE`
  derived from image tags.
- **Chart version** - reset to the vayu release line (chart `0.1.0`).

## Platform release manifest

This repo hosts [`vayu-release.yaml`](./vayu-release.yaml) - the umbrella manifest that pins the
four component versions validated together, tagged `vayu-platform-<X.Y.Z>`. Promote environments by
platform version rather than bumping components individually.

## Versioning

- Release tags: `vayu-<X.Y.Z>+olake<upstream-base>` (e.g. `vayu-0.1.0+olake0.3.10`).
- Image tags map `+` → `-`.

## Branch model & contributing

- `upstream` / `master` - pristine upstream mirror; merge base for syncs. Never commit here.
- `vayu-main` - protected integration/release branch. **Direct pushes are blocked**; changes land
  via pull requests.
- `feature/*` - branch off `vayu-main`, PR back in, updating `VAYU.md`, `CHANGELOG.md`, and (on a
  release) `vayu-release.yaml`.
- Sync upstream by merging `upstream` into `vayu-main` (never rebase the release branch).
