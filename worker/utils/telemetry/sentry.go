package telemetry

import (
	"os"
	"regexp"
	"time"

	"github.com/datazip-inc/olake-helm/worker/utils/logger"
	"github.com/getsentry/sentry-go"
)

// Scrub patterns for secrets that commonly leak into error messages:
// credentials embedded in connection URIs, AWS access key ids, and SQL
// following "query:"/"statement:" markers (up to end of line).
var (
	reURICreds    = regexp.MustCompile(`://[^/@\s]+:[^/@\s]+@`)
	reAWSKeyID    = regexp.MustCompile(`\bAKIA[A-Z0-9]{16}\b`)
	reQueryMarker = regexp.MustCompile(`(?i)\b(query|statement):[^\n]*`)
)

// InitSentry configures the global sentry client.
// No-op when SENTRY_DSN is empty, so local/dev runs stay untouched.
func InitSentry() {
	dsn := os.Getenv("SENTRY_DSN")
	if dsn == "" {
		return
	}
	if err := sentry.Init(sentry.ClientOptions{
		Dsn:              dsn,
		Release:          os.Getenv("SENTRY_RELEASE"),
		AttachStacktrace: true,
		TracesSampleRate: 0,
		BeforeSend:       scrubEvent,
	}); err != nil {
		logger.Warnf("failed to initialize sentry: %s", err)
		return
	}
	logger.Infof("sentry initialized")
}

// FlushSentry drains buffered sentry events. Safe when sentry is disabled.
func FlushSentry(timeout time.Duration) {
	sentry.Flush(timeout)
}

// CaptureError reports err to sentry with tags and extra details
// (sentry-go >= v0.48 replaced event "extras" with contexts).
// No-op when sentry was not initialized.
func CaptureError(err error, tags map[string]string, extras map[string]interface{}) {
	sentry.WithScope(func(scope *sentry.Scope) {
		scope.SetTags(tags)
		if len(extras) > 0 {
			scope.SetContext("details", sentry.Context(extras))
		}
		sentry.CaptureException(err)
	})
}

// CaptureErrorMessage reports msg to sentry at level error with tags.
// No-op when sentry was not initialized.
func CaptureErrorMessage(msg string, tags map[string]string) {
	sentry.WithScope(func(scope *sentry.Scope) {
		scope.SetTags(tags)
		scope.SetLevel(sentry.LevelError)
		sentry.CaptureMessage(msg)
	})
}

func scrubString(s string) string {
	s = reURICreds.ReplaceAllString(s, "://***@")
	s = reAWSKeyID.ReplaceAllString(s, "AKIA****************")
	s = reQueryMarker.ReplaceAllString(s, "${1}: [redacted]")
	return s
}

// scrubEvent is the BeforeSend hook: redacts secrets from messages, exception
// values, and string context values (pod log tails) before events leave the
// process.
func scrubEvent(event *sentry.Event, _ *sentry.EventHint) *sentry.Event {
	event.Message = scrubString(event.Message)
	for i := range event.Exception {
		event.Exception[i].Value = scrubString(event.Exception[i].Value)
	}
	for _, context := range event.Contexts {
		for k, v := range context {
			if s, ok := v.(string); ok {
				context[k] = scrubString(s)
			}
		}
	}
	return event
}
