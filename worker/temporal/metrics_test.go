package temporal

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/datazip-inc/olake-helm/worker/metrics"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

// runSyncWorkflow executes RunSyncWorkflow with a stubbed sync activity and
// returns the request the deferred cleanup (PostSyncActivity) received.
func runSyncWorkflow(t *testing.T, syncErr error) *types.ExecutionRequest {
	t.Helper()
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(RunSyncWorkflow)

	env.RegisterActivityWithOptions(func(context.Context, *types.ExecutionRequest) (*types.ExecutorResponse, error) {
		if syncErr != nil {
			return nil, syncErr
		}
		return &types.ExecutorResponse{Response: "ok"}, nil
	}, activity.RegisterOptions{Name: SyncActivity})

	var cleanupReq *types.ExecutionRequest
	env.RegisterActivityWithOptions(func(_ context.Context, req *types.ExecutionRequest) error {
		cleanupReq = req
		return nil
	}, activity.RegisterOptions{Name: PostSyncActivity})

	env.RegisterActivityWithOptions(func(context.Context, types.WebhookNotificationArgs) error {
		return nil
	}, activity.RegisterOptions{Name: SendWebhookNotificationActivity})

	env.ExecuteWorkflow(RunSyncWorkflow, map[string]interface{}{"command": "sync", "job_id": 42})
	require.True(t, env.IsWorkflowCompleted())
	require.NotNil(t, cleanupReq, "cleanup activity was not executed")
	return cleanupReq
}

func TestRunSyncWorkflowPlumbsSyncFailedToCleanup(t *testing.T) {
	req := runSyncWorkflow(t, nil)
	require.False(t, req.SyncFailed, "successful sync must reach cleanup with SyncFailed=false")

	req = runSyncWorkflow(t, temporal.NewNonRetryableApplicationError("pod failed: OOMKilled", "OOMKilled", nil))
	require.True(t, req.SyncFailed, "failed sync must reach cleanup with SyncFailed=true")
}

func TestReadSyncedRecords(t *testing.T) {
	workflowID := "metrics-test-workflow"
	_, workdir := utils.GetWorkflowDirAndSubDir(workflowID, types.Sync)
	require.NoError(t, os.MkdirAll(workdir, 0o755))
	t.Cleanup(func() { _ = os.RemoveAll(workdir) })

	stats := `{"Estimated Remaining Time":"0s","Memory":"120mb","Running Threads":0,"Seconds Elapsed":42,"Speed":"2938 rps","Synced Records":123456}`
	require.NoError(t, os.WriteFile(filepath.Join(workdir, "stats.json"), []byte(stats), 0o644))

	records, err := readSyncedRecords(workflowID, types.Sync)
	require.NoError(t, err)
	require.Equal(t, float64(123456), records)

	_, err = readSyncedRecords("no-such-workflow", types.Sync)
	require.Error(t, err, "missing stats.json must surface as an error, not a zero count")
}

// TestMetricsExposition records the same observations the activities make and
// verifies they appear on the /metrics endpoint promhttp serves (health.go).
func TestMetricsExposition(t *testing.T) {
	metrics.SyncDurationSeconds.WithLabelValues("42", "demo-job", "postgres", "success").Observe(37.5)
	metrics.SyncDurationSeconds.WithLabelValues("42", "demo-job", "postgres", "failed").Observe(912)
	metrics.RecordsSyncedTotal.WithLabelValues("42", "demo-job", "postgres").Add(123456)
	metrics.JobRunsTotal.WithLabelValues("42", "demo-job", "postgres", "success").Inc()

	rec := httptest.NewRecorder()
	promhttp.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()

	require.Contains(t, body, `olake_sync_duration_seconds_bucket{connector_type="postgres",job_id="42",job_name="demo-job",status="success",le="60"} 1`)
	require.Contains(t, body, `olake_sync_duration_seconds_count{connector_type="postgres",job_id="42",job_name="demo-job",status="failed"} 1`)
	require.Contains(t, body, `olake_records_synced_total{connector_type="postgres",job_id="42",job_name="demo-job"} 123456`)

	if f := os.Getenv("METRICS_EVIDENCE_FILE"); f != "" {
		var olake []string
		for _, line := range strings.Split(body, "\n") {
			if strings.Contains(line, "olake_") {
				olake = append(olake, line)
			}
		}
		require.NoError(t, os.WriteFile(f, []byte(strings.Join(olake, "\n")+"\n"), 0o644))
	}
}
