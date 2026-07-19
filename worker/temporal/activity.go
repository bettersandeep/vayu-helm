package temporal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/database"
	"github.com/datazip-inc/olake-helm/worker/executor"
	"github.com/datazip-inc/olake-helm/worker/metrics"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
	"github.com/datazip-inc/olake-helm/worker/utils/notifications"
	"github.com/datazip-inc/olake-helm/worker/utils/telemetry"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
)

type Activity struct {
	executor   *executor.AbstractExecutor
	db         *database.DB
	tempClient client.Client
}

func NewActivity(e *executor.AbstractExecutor, db *database.DB, c *Temporal) *Activity {
	return &Activity{executor: e, db: db, tempClient: c.GetClient()}
}

func (a *Activity) ExecuteActivity(ctx context.Context, req *types.ExecutionRequest) (*types.ExecutorResponse, error) {
	log := logger.Log(ctx)
	log.Info("executing activity",
		"command", req.Command,
		"sourceType", req.ConnectorType,
		"version", req.Version,
		"workflowID", req.WorkflowID,
	)

	activity.RecordHeartbeat(ctx, "executing %s activity", req.Command)
	req.HeartbeatFunc = activity.RecordHeartbeat

	if req.Command == types.ClearDestination {
		jobDetails, err := a.db.GetJobData(ctx, req.JobID)
		if err != nil {
			return nil, err
		}

		if err := utils.UpdateConfigForClearDestination(jobDetails, req); err != nil {
			return nil, err
		}
	}

	return a.executor.Execute(ctx, req)
}

func (a *Activity) SyncActivity(ctx context.Context, req *types.ExecutionRequest) (*types.ExecutorResponse, error) {
	log := logger.Log(ctx)
	log.Info("executing sync activity", "jobID", req.JobID)

	// Record heartbeat before execution
	activity.RecordHeartbeat(ctx, "executing sync for job %d", req.JobID)
	req.HeartbeatFunc = activity.RecordHeartbeat

	// Update the configs with latest
	jobDetails, err := a.db.GetJobData(ctx, req.JobID)
	if err != nil {
		errMsg := fmt.Sprintf("failed to get job data: %s", err)
		// job_name is unavailable here: fetching job data is what failed.
		telemetry.CaptureError(err, map[string]string{
			"connector_type": req.ConnectorType,
			"job_id":         strconv.Itoa(req.JobID),
			"error_type":     "DatabaseError",
		}, nil)
		return nil, temporal.NewNonRetryableApplicationError(errMsg, "DatabaseError", err)
	}

	// mapping request type of deprecated workflow to new request type
	// old scheduled sync workflow has no connector type set
	if req.ConnectorType == "" {
		utils.UpdateSyncRequestForLegacy(jobDetails, req)
	}

	// update the configs with latest job details
	utils.UpdateConfigWithJobDetails(jobDetails, req)

	// Remove --state flag if state is empty
	if utils.IsStateEmpty(jobDetails.State) {
		req.Args = utils.RemoveFlagFromArgs(req.Args, constants.StateFlag)
	}

	// Send telemetry event - "sync started"
	telemetry.SendEvent(req.JobID, utils.GetExecutorEnvironment(), req.WorkflowID, telemetry.TelemetryEventStarted)

	syncStart := time.Now()
	result, err := a.executor.Execute(ctx, req)
	syncDuration := time.Since(syncStart).Seconds()
	if err != nil {
		// CRITICAL: Check if error is because context was cancelled
		if ctx.Err() != nil {
			log.Info("sync activity cancelled", "jobID", req.JobID)
			return nil, temporal.NewCanceledError("sync activity cancelled")
		}

		telemetry.SendEvent(req.JobID, utils.GetExecutorEnvironment(), req.WorkflowID, telemetry.TelemetryEventFailed)

		// Classify the error type using the structured PodFailureError if available.
		// The type string becomes appErr.Type() in the workflow, which is then forwarded
		// to the webhook. Using the raw k8s Reason string means any future Kubernetes
		// termination reason (beyond the ones we know today) is automatically carried
		// through without requiring code changes.
		errorType := "ApplicationError"
		var podErr *constants.PodFailureError
		if errors.As(err, &podErr) {
			if podErr.Reason != "" {
				errorType = podErr.Reason // e.g. "OOMKilled", "Error", "DeadlineExceeded"
			} else {
				errorType = "UnknownPodFailure"
			}
		}

		// Increment Prometheus counters so failures are observable in Grafana.
		// jobDetails is guaranteed non-nil here (fetched above, error returned early).
		jobIDStr := strconv.Itoa(req.JobID)
		metrics.JobRunsTotal.WithLabelValues(jobIDStr, jobDetails.JobName, req.ConnectorType, "failed").Inc()
		metrics.JobFailuresTotal.WithLabelValues(jobIDStr, jobDetails.JobName, req.ConnectorType, errorType).Inc()
		metrics.SyncDurationSeconds.WithLabelValues(jobIDStr, jobDetails.JobName, req.ConnectorType, "failed").Observe(syncDuration)

		// Build the full message that will survive the Temporal serialisation boundary.
		// err.Error() from PodFailureError already contains reason and exit code.
		// We append pod logs here because the cause chain is not reliably accessible
		// across the workflow→activity boundary via errors.As.
		var msgParts []string
		msgParts = append(msgParts, err.Error())
		if podErr != nil && podErr.PodLogs != "" {
			msgParts = append(msgParts, "\n--- Pod Logs (last output before failure) ---\n"+notifications.TailLines(podErr.PodLogs, 30))
		}
		fullMsg := strings.Join(msgParts, "\n")

		// Capture to sentry with the classified error type. The catch-all
		// interceptor skips ApplicationErrors, so this is the single capture
		// point for classified sync failures.
		sentryExtras := map[string]interface{}{}
		if podErr != nil && podErr.PodLogs != "" {
			sentryExtras["pod_log_tail"] = notifications.TailLines(podErr.PodLogs, 50)
		}
		telemetry.CaptureError(err, map[string]string{
			"connector_type": req.ConnectorType,
			"job_id":         jobIDStr,
			"job_name":       jobDetails.JobName,
			"error_type":     errorType,
		}, sentryExtras)

		log.Error("sync command failed", "jobID", req.JobID, "errorType", errorType, "error", err)
		return nil, temporal.NewNonRetryableApplicationError(fullMsg, errorType, err)
	}

	metrics.SyncDurationSeconds.WithLabelValues(
		strconv.Itoa(req.JobID), jobDetails.JobName, req.ConnectorType, "success",
	).Observe(syncDuration)

	return result, nil
}

func (a *Activity) PostSyncActivity(ctx context.Context, req *types.ExecutionRequest) error {
	log := logger.Log(ctx)
	log.Info("cleaning up sync for job", "jobID", req.JobID)

	jobDetails, err := a.db.GetJobData(ctx, req.JobID)
	if err != nil {
		return err
	}

	if req.ConnectorType == "" {
		utils.UpdateSyncRequestForLegacy(jobDetails, req)
	}

	if err := a.executor.CleanupAndPersistState(ctx, req); err != nil {
		return temporal.NewNonRetryableApplicationError(err.Error(), "cleanup failed", err)
	}

	// The workflow must stay deterministic, so the "sync failed" sentry event
	// is emitted here on the activity side from the SyncFailed flag the
	// workflow plumbed into cleanup.
	if req.SyncFailed {
		telemetry.CaptureErrorMessage("sync failed: "+jobDetails.JobName, map[string]string{
			"connector_type": req.ConnectorType,
			"job_id":         strconv.Itoa(req.JobID),
			"job_name":       jobDetails.JobName,
		})
	}

	// Increment Prometheus success counter. jobDetails is non-nil (fetched above).
	// This cleanup activity runs on every outcome (failure and cancellation
	// included), so success-only metrics are gated on the sync result the
	// workflow plumbed through req.SyncFailed.
	if !req.SyncFailed {
		metrics.JobRunsTotal.WithLabelValues(
			strconv.Itoa(req.JobID), jobDetails.JobName, req.ConnectorType, "success",
		).Inc()

		// Best-effort: read record count from the stats.json the CLI wrote to the
		// shared volume. Never fails the activity — a missing file just means no
		// records metric for this run.
		if records, err := readSyncedRecords(req.WorkflowID, req.Command); err != nil {
			log.Warn("could not read synced records from stats.json", "jobID", req.JobID, "error", err)
		} else {
			metrics.RecordsSyncedTotal.WithLabelValues(
				strconv.Itoa(req.JobID), jobDetails.JobName, req.ConnectorType,
			).Add(records)
		}
	}

	telemetry.SendEvent(req.JobID, utils.GetExecutorEnvironment(), req.WorkflowID, telemetry.TelemetryEventCompleted)
	return nil
}

// readSyncedRecords parses "Synced Records" from the stats.json written every
// 2s by the olake CLI at the root of the job's shared-volume directory.
func readSyncedRecords(workflowID string, command types.Command) (float64, error) {
	_, workdir := utils.GetWorkflowDirAndSubDir(workflowID, command)
	data, err := os.ReadFile(filepath.Join(workdir, "stats.json"))
	if err != nil {
		return 0, err
	}
	var stats struct {
		SyncedRecords float64 `json:"Synced Records"`
	}
	if err := json.Unmarshal(data, &stats); err != nil {
		return 0, err
	}
	return stats.SyncedRecords, nil
}

// CRITICAL: Restore the schedule to its normal sync operation state
//
// When clear-destination is triggered, the backend (olake-ui) temporarily:
// 1. Updates the sync schedule's metadata to run clear-destination instead
// 2. Pauses the schedule to prevent the next scheduled run during the operation
//
// After clear-destination completes (success or failure), we must restore the schedule:
// 1. Revert metadata back to sync operation
// 2. Unpause the schedule to resume normal operations
//
// Without these steps, the schedule would remain paused and stuck in clear-destination mode,
// preventing all future sync runs.
func (a *Activity) PostClearActivity(ctx context.Context, req *types.ExecutionRequest) error {
	log := logger.Log(ctx)
	log.Info("cleaning up clear-destination for job", "jobID", req.JobID)

	if err := a.executor.CleanupAndPersistState(ctx, req); err != nil {
		return err
	}

	utils.RevertUpdatesInSchedule(req)

	// update the schedule
	workflowID := fmt.Sprintf("sync-%s-%d", req.ProjectID, req.JobID)
	scheduleID := fmt.Sprintf("schedule-%s", workflowID)
	handle := a.tempClient.ScheduleClient().GetHandle(ctx, scheduleID)

	taskQueue := utils.GetTemporalTaskQueue()

	err := handle.Update(ctx, client.ScheduleUpdateOptions{
		DoUpdate: func(input client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
			input.Description.Schedule.Action = &client.ScheduleWorkflowAction{
				ID:        workflowID,
				Workflow:  RunSyncWorkflow,
				Args:      []any{req},
				TaskQueue: taskQueue,
			}

			if input.Description.Schedule.State != nil {
				input.Description.Schedule.State.Paused = false
				input.Description.Schedule.State.Note = "Restored to sync after clear-destination"
			}

			return &client.ScheduleUpdate{
				Schedule: &input.Description.Schedule,
			}, nil
		},
	})
	if err != nil {
		log.Error("failed to update schedule", "jobID", req.JobID, "scheduleID", scheduleID, "error", err)
		return err
	}

	// Verify the schedule is actually unpaused
	desc, err := handle.Describe(ctx)
	if err != nil {
		log.Error("failed to describe schedule after update", "jobID", req.JobID, "scheduleID", scheduleID, "error", err)
		return err
	}
	if desc.Schedule.State.Paused {
		log.Error("schedule still paused after update", "jobID", req.JobID, "scheduleID", scheduleID)
		return fmt.Errorf("schedule %s, jobID: %d still paused after update", scheduleID, req.JobID)
	}

	log.Info("successfully updated schedule (clear-destination to sync)", "jobID", req.JobID, "scheduleID", scheduleID)

	return nil
}

func (a *Activity) SendWebhookNotificationActivity(ctx context.Context, req types.WebhookNotificationArgs) error {
	log := logger.Log(ctx)
	log.Info("Sending webhook alert", "jobID", req.JobID, "projectID", req.ProjectID)

	projectID := req.ProjectID
	if projectID == "" {
		// TODO: introduce a dedicated migration to backfill project_id into schedules for older jobs and remove this hardcoded fallback.
		projectID = "123"
		log.Info("project_id is empty, defaulting to fallback project_id", "jobID", req.JobID, "fallbackProjectID", projectID)
	}

	settings, err := a.db.GetProjectSettingsByProjectID(ctx, projectID)
	if err != nil {
		return fmt.Errorf("failed to get project settings: %w", err)
	}

	jobDetails, err := a.db.GetJobData(ctx, req.JobID)
	if err != nil {
		log.Warn("failed to get job data for webhook notification", "jobID", req.JobID, "error", err)
	}
	jobName := jobDetails.JobName

	if err := notifications.SendWebhookNotification(ctx, req, jobName, settings.WebhookAlertURL); err != nil {
		return fmt.Errorf("failed to send webhook notification: %w", err)
	}
	return nil
}
