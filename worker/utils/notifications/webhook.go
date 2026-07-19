package notifications

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/datazip-inc/olake-helm/worker/types"
)

// slackMessage is the top-level payload sent to a Slack Incoming Webhook.
// `text` is a short one-line fallback shown in push notifications and in clients
// that do not render Block Kit.  `blocks` carries the rich formatted content.
type slackMessage struct {
	Text   string                   `json:"text"`
	Blocks []map[string]interface{} `json:"blocks,omitempty"`
}

// SendWebhookNotification sends a structured Block Kit alert to the Slack
// Incoming Webhook URL stored in project settings.
func SendWebhookNotification(ctx context.Context, req types.WebhookNotificationArgs, jobName, webhookURL string) error {
	if strings.TrimSpace(webhookURL) == "" {
		return fmt.Errorf("webhook_alert_url not configured")
	}

	failureLabel := failureTypeDisplayName(req.ErrorType)
	errorDesc := formatErrorForAlert(req)
	podLogs := lastOutputFromMessage(req.ErrorMessage)
	runTime := req.LastRunTime.Format("2006-01-02 15:04:05 UTC")

	// Short fallback text for push notifications / accessibility.
	fallbackText := fmt.Sprintf("🚨 Sync Failure: %s (%s)", jobName, failureLabel)

	blocks := []map[string]interface{}{
		headerBlock("🚨  Sync Failure Detected"),
		dividerBlock(),
		// Two-column field grid: Job ID | Job Name / Failure Type | Exit Code
		fieldsBlock([][2]string{
			{"*Job ID*", fmt.Sprintf("`%d`", req.JobID)},
			{"*Job Name*", fmt.Sprintf("`%s`", jobName)},
			{"*Failure Type*", fmt.Sprintf("`%s`", failureLabel)},
			{"*Exit Code*", fmt.Sprintf("`%d`", req.ExitCode)},
		}),
		dividerBlock(),
		// Error description — plain text, no inline code flooding
		sectionBlock("*Error*\n" + errorDesc),
	}

	// Pod logs block — only added when there is actual log output.
	// Wrapped in triple-backtick so Slack renders a scrollable monospace code box.
	// Capped at 2800 chars to stay within Slack's 3000-char block limit.
	if podLogs != "" {
		capped := capString(podLogs, 2800)
		blocks = append(blocks, sectionBlock("*Last pod output*\n```"+capped+"```"))
	}

	blocks = append(blocks,
		dividerBlock(),
		// Footer context: timestamp + pod name in smaller secondary text
		contextBlock(
			fmt.Sprintf("🕐  %s", runTime),
			fmt.Sprintf("📦  Pod: %s", podNameOrUnknown(req.PodName)),
		),
	)

	msg := slackMessage{
		Text:   fallbackText,
		Blocks: blocks,
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal slack message: %w", err)
	}

	resp, err := http.Post(webhookURL, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("failed to send webhook notification: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("webhook returned non-2xx status: %s", resp.Status)
	}
	return nil
}

// ── Block Kit builder helpers ────────────────────────────────────────────────

// headerBlock renders bold large text at the top of the message.
// Slack enforces a 150-character limit on header text.
func headerBlock(text string) map[string]interface{} {
	return map[string]interface{}{
		"type": "header",
		"text": map[string]interface{}{
			"type":  "plain_text",
			"text":  capString(text, 150),
			"emoji": true,
		},
	}
}

// sectionBlock renders a markdown text section.
// Slack enforces a 3000-character limit; capString enforces that upstream.
func sectionBlock(text string) map[string]interface{} {
	return map[string]interface{}{
		"type": "section",
		"text": map[string]interface{}{
			"type": "mrkdwn",
			"text": text,
		},
	}
}

// fieldsBlock renders pairs of [label, value] as a two-column grid inside a
// single section block.  Slack shows up to 10 fields per block; we use at most 4.
func fieldsBlock(pairs [][2]string) map[string]interface{} {
	fields := make([]map[string]interface{}, 0, len(pairs)*2)
	for _, p := range pairs {
		fields = append(fields,
			map[string]interface{}{"type": "mrkdwn", "text": p[0]},
			map[string]interface{}{"type": "mrkdwn", "text": p[1]},
		)
	}
	return map[string]interface{}{
		"type":   "section",
		"fields": fields,
	}
}

// dividerBlock inserts a horizontal rule between sections.
func dividerBlock() map[string]interface{} {
	return map[string]interface{}{"type": "divider"}
}

// contextBlock renders one or more strings as small secondary text at the bottom
// of the message — useful for timestamps and pod identifiers.
func contextBlock(elements ...string) map[string]interface{} {
	elems := make([]map[string]interface{}, 0, len(elements))
	for _, e := range elements {
		elems = append(elems, map[string]interface{}{
			"type": "mrkdwn",
			"text": e,
		})
	}
	return map[string]interface{}{
		"type":     "context",
		"elements": elems,
	}
}

// ── Error content helpers ────────────────────────────────────────────────────

// formatErrorForAlert returns an actionable plain-text description of the failure.
// For known pod-level failure types (OOMKilled, Evicted) it returns a structured
// message with a remediation hint.  For application errors it scans the error
// message for FATAL/ERROR log lines, falling back to the raw error string.
// The pod logs section is handled separately in SendWebhookNotification so that
// it can be placed in its own code-block section.
func formatErrorForAlert(req types.WebhookNotificationArgs) string {
	switch req.ErrorType {
	case "OOMKilled":
		return fmt.Sprintf(
			"Pod was killed due to Out-Of-Memory (OOMKilled, exit code %d).\n"+
				"The connector exhausted the available memory on its node.\n"+
				"Consider increasing `jobPodResources.memory` in your Helm values.",
			req.ExitCode,
		)

	case "Evicted":
		return fmt.Sprintf(
			"Pod was evicted from the node (exit code %d).\n"+
				"This is typically caused by node-level memory or disk pressure "+
				"and is not an application error.\n"+
				"The sync will be retried on the next scheduled run.",
			req.ExitCode,
		)

	default:
		// For application errors: scan the Temporal error message for FATAL/ERROR lines.
		filtered := trimErrorLogs(req.ErrorMessage)
		if filtered != "No critical error lines found. See full logs for details." {
			return filtered
		}
		// Nothing useful found — show the raw error string so the user always sees
		// something concrete rather than the generic fallback.
		raw := strings.TrimSpace(req.ErrorMessage)
		if raw == "" {
			return "No error details available. Check the pod logs directly."
		}
		return capString(raw, 2000)
	}
}

// trimErrorLogs scans multi-line log output and returns only FATAL/ERROR lines.
func trimErrorLogs(logs string) string {
	lines := strings.Split(logs, "\n")
	var filtered []string
	for _, line := range lines {
		if strings.Contains(line, "FATAL") || strings.Contains(line, "ERROR") {
			filtered = append(filtered, line)
		}
	}
	if len(filtered) == 0 {
		return "No critical error lines found. See full logs for details."
	}
	return strings.Join(filtered, "\n")
}

// lastOutputFromMessage extracts the pod-logs section that SyncActivity appends
// to the Temporal error message after the sentinel marker.
// Returns an empty string if the marker is absent (i.e. no pod logs were captured).
func lastOutputFromMessage(msg string) string {
	const marker = "--- Pod Logs (last output before failure) ---"
	idx := strings.Index(msg, marker)
	if idx < 0 {
		return ""
	}
	return strings.TrimSpace(msg[idx+len(marker):])
}

// TailLines returns the last n lines of s.
// Exported so SyncActivity can use it without creating a circular import.
func TailLines(s string, n int) string {
	if s == "" {
		return ""
	}
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) <= n {
		return s
	}
	return strings.Join(lines[len(lines)-n:], "\n")
}

// ── Display helpers ──────────────────────────────────────────────────────────

// failureTypeDisplayName converts a raw k8s termination reason into a
// human-readable label.  Unknown reasons are passed through verbatim so new
// Kubernetes termination reasons surface without requiring a code change.
func failureTypeDisplayName(errorType string) string {
	switch errorType {
	case "OOMKilled":
		return "Out-Of-Memory Kill"
	case "Evicted":
		return "Pod Eviction (node pressure)"
	case "ApplicationError":
		return "Application Error"
	case "UnknownPodFailure":
		return "Unknown Pod Failure"
	case "":
		return "Unknown"
	default:
		return errorType
	}
}

func podNameOrUnknown(name string) string {
	if name == "" {
		return "unknown"
	}
	return name
}

// capString truncates s to at most n bytes, appending an ellipsis when truncated.
func capString(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "… (truncated)"
}
