package types

import "time"

type Command string

const (
	Discover         Command = "discover"
	Spec             Command = "spec"
	Check            Command = "check"
	Sync             Command = "sync"
	ClearDestination Command = "clear-destination"
)

type JobConfig struct {
	Name string `json:"name"`
	Data string `json:"data"`
}

// FileConfig represents a configuration file to be written
type FileConfig struct {
	Name string
	Data string
}

// LoggingConfig contains logging settings
type LoggingConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

type JobData struct {
	JobName     string
	ProjectID   string
	Source      string
	Destination string
	Streams     string
	State       string
	Version     string
	Driver      string
}

type WebhookNotificationArgs struct {
	JobID        int
	ProjectID    string
	LastRunTime  time.Time
	ErrorMessage string // full error string from err.Error() — survives the Temporal boundary

	// Structured fields populated when the error originates from a Kubernetes pod failure.
	// ErrorType is the raw k8s ContainerStateTerminated.Reason ("OOMKilled", "Error", etc.)
	// or "ApplicationError" for non-pod errors.  An empty string means unknown.
	ErrorType string
	// ExitCode is the container exit code: 137=OOMKill/SIGKILL, 1=app error, 143=SIGTERM.
	ExitCode int32
	// PodName is the name of the Kubernetes pod that failed.
	PodName string
}

type Result struct {
	OK      bool
	Message string
}

type ProjectSettings struct {
	ID              int
	ProjectID       string
	WebhookAlertURL string
}
