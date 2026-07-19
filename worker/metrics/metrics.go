package metrics

import "github.com/prometheus/client_golang/prometheus"

// Job-level metrics. All vectors use job_id + job_name + connector_type as base
// labels so every time-series is uniquely identifiable in Grafana.
//
// Useful PromQL queries:
//
//	# Jobs that failed in the last hour
//	increase(olake_job_runs_total{status="failed"}[1h]) > 0
//
//	# OOM kill rate by connector type
//	rate(olake_job_failures_total{error_type="OOMKilled"}[24h])
//
//	# Success rate over 24 h
//	sum(rate(olake_job_runs_total{status="success"}[24h]))
//	  / sum(rate(olake_job_runs_total[24h]))
var (
	// JobRunsTotal counts every sync run completion, tagged with its outcome.
	// Incremented in PostSyncActivity on success and in SyncActivity on failure.
	//
	// Labels:
	//   job_id         — numeric job identifier (e.g. "34")
	//   job_name       — human-readable job name (e.g. "orders_high_freq_sync")
	//   connector_type — source connector type (e.g. "postgres", "mysql", "mongodb")
	//   status         — "success" or "failed"
	JobRunsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "olake_job_runs_total",
			Help: "Total number of OLake sync job runs, partitioned by outcome.",
		},
		[]string{"job_id", "job_name", "connector_type", "status"},
	)

	// JobFailuresTotal counts every sync failure, additionally tagged with the
	// specific error type so OOM kills, evictions and application crashes can be
	// tracked separately.
	// Incremented alongside JobRunsTotal{status="failed"} in SyncActivity.
	//
	// Labels:
	//   job_id         — numeric job identifier
	//   job_name       — human-readable job name
	//   connector_type — source connector type
	//   error_type     — raw k8s Reason or "ApplicationError":
	//                    "OOMKilled", "Evicted", "ApplicationError",
	//                    "UnknownPodFailure", or any future k8s termination reason
	JobFailuresTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "olake_job_failures_total",
			Help: "Total number of OLake sync job failures, partitioned by error type.",
		},
		[]string{"job_id", "job_name", "connector_type", "error_type"},
	)

	// SyncDurationSeconds measures wall-clock time of the sync execution
	// (executor.Execute in SyncActivity), observed on both success and failure.
	//
	// Labels: same base as JobRunsTotal, plus status ("success"/"failed").
	//
	//	# P95 sync duration per job over 24h
	//	histogram_quantile(0.95, sum by (job_name, le) (rate(olake_sync_duration_seconds_bucket[24h])))
	SyncDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "olake_sync_duration_seconds",
			Help:    "Wall-clock duration of sync job executions in seconds.",
			Buckets: []float64{30, 60, 300, 900, 1800, 3600, 7200, 14400},
		},
		[]string{"job_id", "job_name", "connector_type", "status"},
	)

	// RecordsSyncedTotal counts records synced per run, read from the stats.json
	// the CLI writes to the shared volume. Incremented in PostSyncActivity on success.
	//
	//	# Records synced per day per job
	//	increase(olake_records_synced_total[1d])
	//
	//	# Average throughput (records/sec) per job over 24h
	//	increase(olake_records_synced_total[24h]) / increase(olake_sync_duration_seconds_sum{status="success"}[24h])
	RecordsSyncedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "olake_records_synced_total",
			Help: "Total number of records synced across job runs.",
		},
		[]string{"job_id", "job_name", "connector_type"},
	)
)

func init() {
	prometheus.MustRegister(JobRunsTotal, JobFailuresTotal, SyncDurationSeconds, RecordsSyncedTotal)
}
