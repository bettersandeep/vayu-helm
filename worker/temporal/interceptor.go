package temporal

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
	"github.com/datazip-inc/olake-helm/worker/utils/telemetry"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
)

// LoggingInterceptor sets up workflow file logging for activities and acts as
// the catch-all sentry capture for activity panics and unclassified errors.
type LoggingInterceptor struct {
	interceptor.WorkerInterceptorBase
}

func NewLoggingInterceptor() *LoggingInterceptor {
	return &LoggingInterceptor{}
}

func (i *LoggingInterceptor) InterceptActivity(
	ctx context.Context,
	next interceptor.ActivityInboundInterceptor,
) interceptor.ActivityInboundInterceptor {
	return &loggingActivityInterceptor{
		ActivityInboundInterceptorBase: interceptor.ActivityInboundInterceptorBase{Next: next},
	}
}

type loggingActivityInterceptor struct {
	interceptor.ActivityInboundInterceptorBase
}

func (a *loggingActivityInterceptor) ExecuteActivity(
	ctx context.Context,
	in *interceptor.ExecuteActivityInput,
) (interface{}, error) {
	req := extractExecutionRequest(in.Args)
	if req != nil && req.WorkflowID != "" {
		ctxWithLogger, logFile, err := utils.PrepareWorkflowLogger(ctx, req.WorkflowID, req.Command)
		if err != nil {
			logger.Warnf("failed to prepare workflow logger for workflowID=%s: %s", req.WorkflowID, err)
		} else {
			defer logFile.Close()
			ctx = ctxWithLogger
		}
	}
	return a.executeWithSentry(ctx, in, req)
}

// executeWithSentry is the catch-all sentry capture for activities.
// ApplicationErrors are skipped: they were already classified and captured at
// the classification site (activity.go), capturing them again would duplicate
// events. Cancellations are user/workflow initiated, not errors.
func (a *loggingActivityInterceptor) executeWithSentry(
	ctx context.Context,
	in *interceptor.ExecuteActivityInput,
	req *types.ExecutionRequest,
) (out interface{}, err error) {
	defer func() {
		if p := recover(); p != nil {
			telemetry.CaptureError(fmt.Errorf("activity panic: %v", p), sentryTags(req, "panic"), nil)
			panic(p) // re-panic so temporal's own panic handling proceeds
		}
	}()

	out, err = a.Next.ExecuteActivity(ctx, in)

	var appErr *temporal.ApplicationError
	if err != nil && !errors.As(err, &appErr) && !temporal.IsCanceledError(err) {
		telemetry.CaptureError(err, sentryTags(req, "unclassified"), nil)
	}
	return out, err
}

func sentryTags(req *types.ExecutionRequest, errorType string) map[string]string {
	tags := map[string]string{"error_type": errorType}
	if req != nil {
		tags["connector_type"] = req.ConnectorType
		tags["job_id"] = strconv.Itoa(req.JobID)
	}
	return tags
}

func extractExecutionRequest(args []interface{}) *types.ExecutionRequest {
	for _, arg := range args {
		if req, ok := arg.(*types.ExecutionRequest); ok {
			return req
		}
	}
	return nil
}
