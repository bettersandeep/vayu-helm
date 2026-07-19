package constants

import (
	"errors"
	"fmt"
)

// ErrExecutionFailed is returned when a container/pod fails due to non-retryable application errors.
// Infrastructure failures (evictions, image pull errors, etc.) are NOT wrapped with this error.
var ErrExecutionFailed = errors.New("execution failed")

// PodFailureError carries structured failure information from a Kubernetes pod termination.
// It wraps ErrExecutionFailed via Unwrap() so existing errors.Is(err, ErrExecutionFailed)
// checks in activity.go continue to work without modification.
//
// Use errors.As(err, &podErr) downstream to extract structured fields for richer alerting.
type PodFailureError struct {
	PodName  string
	Reason   string // raw k8s ContainerStateTerminated.Reason: "OOMKilled", "Error", "Evicted", etc.
	ExitCode int32
	// Message is populated from ContainerStateTerminated.Message when
	// TerminationMessageFallbackToLogsOnError is set on the container — it contains
	// the last bytes of stdout/stderr for processes that crash without writing to
	// /dev/termination-log (e.g. OOMKilled).
	Message string
	// PodLogs contains the last N lines of the pod's stdout fetched after failure.
	// May be empty if the pod was OOMKilled before flushing output.
	PodLogs string
}

func (e *PodFailureError) Error() string {
	msg := fmt.Sprintf("pod %s failed: reason=%s exitCode=%d", e.PodName, e.Reason, e.ExitCode)
	if e.Message != "" {
		msg += fmt.Sprintf(", message=%s", e.Message)
	}
	return msg
}

// Unwrap makes errors.Is(err, ErrExecutionFailed) return true for *PodFailureError,
// preserving backward compatibility with the existing check in SyncActivity.
func (e *PodFailureError) Unwrap() error {
	return ErrExecutionFailed
}

func (e *PodFailureError) IsOOMKill() bool  { return e.Reason == "OOMKilled" }
func (e *PodFailureError) IsEviction() bool { return e.Reason == "Evicted" }
