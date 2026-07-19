package kubernetes

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
)

func (k *KubernetesExecutor) waitForPodCompletion(ctx context.Context, podName string, timeout time.Duration, heartbeatFunc func(context.Context, ...interface{})) error {
	log := logger.Log(ctx)
	log.Debug("waiting for pod to complete", "podName", podName, "timeout", timeout)
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		// Record heartbeat to enable cancellation detection if heartbeat function is provided
		if heartbeatFunc != nil {
			heartbeatFunc(ctx, fmt.Sprintf("Waiting for pod %s (status check)", podName))
		}

		pod, err := k.client.CoreV1().Pods(k.namespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			log.Error("failed to get pod status", "podName", podName, "error", err)
			return fmt.Errorf("failed to get pod status: %s", err)
		}

		// Check if pod completed successfully
		if pod.Status.Phase == corev1.PodSucceeded {
			log.Info("pod completed successfully", "podName", podName)
			return nil
		}

		// Check if pod failed
		if pod.Status.Phase == corev1.PodFailed {
			// Retryable infrastructure failures — keep polling rather than surfacing as errors.
			// "Evicted" = node memory/disk pressure eviction; will resolve on reschedule.
			retryableReasons := []string{"ImagePullBackOff", "ErrImagePull", "Evicted"}
			if slices.Contains(retryableReasons, pod.Status.Reason) {
				log.Warn("pod not running, continuing to poll", "podName", podName, "reason", pod.Status.Reason, "message", pod.Status.Message)
				continue
			}

			// Build a structured PodFailureError so callers can inspect the exact
			// failure reason (OOMKilled, Error, DeadlineExceeded, etc.) without
			// string-parsing. It wraps ErrExecutionFailed via Unwrap() so existing
			// errors.Is(err, ErrExecutionFailed) checks remain valid.
			//
			// Common exit codes:
			// - Exit 0:   Success
			// - Exit 1:   General application error
			// - Exit 2:   Misuse of shell command
			// - Exit 137: SIGKILL (OOMKilled or manual kill)
			// - Exit 143: SIGTERM (graceful termination)
			podErr := &constants.PodFailureError{PodName: podName}
			if len(pod.Status.ContainerStatuses) > 0 {
				status := pod.Status.ContainerStatuses[0]
				if status.State.Terminated != nil {
					term := status.State.Terminated
					podErr.ExitCode = term.ExitCode
					podErr.Reason = term.Reason
					// term.Message is populated by TerminationMessageFallbackToLogsOnError
					// when the process crashes without writing to /dev/termination-log.
					podErr.Message = term.Message
				}
				// The only other two ContainerState options are Waiting and Running, so if it's not
				// Terminated the fallback below picks up the pod-level reason/message instead.
				// refer: https://pkg.go.dev/k8s.io/api/core/v1#ContainerState
			}
			// Fallback: use pod-level reason/message if container status was unavailable
			if podErr.Reason == "" {
				podErr.Reason = pod.Status.Reason
			}
			if podErr.Message == "" {
				podErr.Message = pod.Status.Message
			}
			log.Error("pod failed", "podName", podName, "reason", podErr.Reason, "exitCode", podErr.ExitCode)
			return podErr
		}

		// Wait before checking again, with responsive cancellation
		select {
		case <-time.After(5 * time.Second):
			// Continue to next iteration
		case <-ctx.Done():
			log.Warn("context cancelled while waiting for pod", "podName", podName)
			return ctx.Err()
		}
	}

	log.Error("pod timed out", "podName", podName, "timeout", timeout)
	return fmt.Errorf("pod timed out after %v", timeout)
}

func (k *KubernetesExecutor) getPodLogs(ctx context.Context, podName string) (string, error) {
	log := logger.Log(ctx)
	req := k.client.CoreV1().Pods(k.namespace).GetLogs(podName, &corev1.PodLogOptions{
		Container: "connector",
	})

	logs, err := req.Stream(ctx)
	if err != nil {
		log.Error("failed to stream pod logs", "podName", podName, "error", err)
		return "", fmt.Errorf("failed to get pod logs: %s", err)
	}
	defer func() {
		if err := logs.Close(); err != nil {
			log.Warn("failed to close log stream", "podName", podName, "error", err)
		}
	}()

	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, logs)
	if err != nil {
		log.Error("failed to read pod logs", "podName", podName, "error", err)
		return "", fmt.Errorf("failed to read pod logs: %s", err)
	}

	return buf.String(), nil
}

func (k *KubernetesExecutor) cleanupPod(ctx context.Context, podName string) error {
	log := logger.Log(ctx)
	log.Debug("cleaning up pod", "podName", podName, "namespace", k.namespace)

	// Delete the pod only
	err := k.client.CoreV1().Pods(k.namespace).Delete(ctx, podName, metav1.DeleteOptions{})
	if err != nil {
		// Treat "not found" as success - cleanup is idempotent
		if apierrors.IsNotFound(err) {
			log.Info("pod already deleted", "podName", podName, "namespace", k.namespace)
			return nil
		}
		log.Error("failed to delete pod", "podName", podName, "namespace", k.namespace, "error", err)
		return fmt.Errorf("failed to delete pod %s in namespace %s: %s", podName, k.namespace, err)
	}

	log.Debug("successfully cleaned up pod", "podName", podName, "namespace", k.namespace)
	return nil
}

func (k *KubernetesExecutor) getJavaHeapOptions() string {
	memRequest := k.config.JobPodMemoryRequest
	if memRequest == "" {
		return "-Xmx4g"
	}

	// Convert the memory request to mebibytes (Mi) regardless of the suffix used.
	// Previously only "Gi" and "G" were handled; "Mi" and "M" fell through to the
	// memGB <= 0 branch and incorrectly returned "-Xmx4g" even for small pods
	// (e.g. the chart default of "256Mi" would get a 4 GB heap, guaranteeing OOM).
	memMi := 0
	switch {
	case strings.HasSuffix(memRequest, "Gi"):
		val, err := strconv.Atoi(strings.TrimSuffix(memRequest, "Gi"))
		if err == nil {
			memMi = val * 1024
		}
	case strings.HasSuffix(memRequest, "G"):
		val, err := strconv.Atoi(strings.TrimSuffix(memRequest, "G"))
		if err == nil {
			memMi = val * 1024
		}
	case strings.HasSuffix(memRequest, "Mi"):
		val, err := strconv.Atoi(strings.TrimSuffix(memRequest, "Mi"))
		if err == nil {
			memMi = val
		}
	case strings.HasSuffix(memRequest, "M"):
		val, err := strconv.Atoi(strings.TrimSuffix(memRequest, "M"))
		if err == nil {
			memMi = val
		}
	}

	if memMi <= 0 {
		return "-Xmx4g"
	}

	// Keep the original formula: reserve 4 GB for JVM non-heap overhead (metaspace,
	// code cache, thread stacks, GC bookkeeping) and give the rest to the heap.
	// Minimum heap is 1 GB so the JVM can start on pods smaller than 5 Gi.
	memGB := memMi / 1024
	heapGB := memGB - 4
	if heapGB < 1 {
		heapGB = 1
	}

	return fmt.Sprintf("-Xmx%dg", heapGB)
}

func (k *KubernetesExecutor) CreatePodSpec(req *types.ExecutionRequest, workDir, imageName string) *corev1.Pod {
	subDir := filepath.Base(workDir)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      k.sanitizeName(req.WorkflowID), // Sanitized name safe for Kubernetes
			Namespace: k.namespace,                    // Target namespace for pod creation

			// Labels are used for querying, filtering, and organizing pods
			Labels: map[string]string{
				// Standard Kubernetes labels for ecosystem compatibility
				"app.kubernetes.io/name":       "olake",                                                      // Application name
				"app.kubernetes.io/component":  fmt.Sprintf("%s-%s", req.ConnectorType, string(req.Command)), // Component identifier
				"app.kubernetes.io/managed-by": "olake-workers",                                              // Management tool

				// Custom Olake labels for internal operations and queries
				"olake.io/operation-type": string(req.Command),            // sync, discover, or check
				"olake.io/connector":      req.ConnectorType,              // mysql, postgres, etc.
				"olake.io/job-id":         strconv.Itoa(req.JobID),        // Database job reference
				"olake.io/workflow-id":    k.sanitizeName(req.WorkflowID), // Sanitized workflow ID
			},

			// Annotations store metadata that doesn't affect pod selection/scheduling.
			// Global job pod annotations (global.podAnnotations) are merged via buildPodAnnotations;
			// olake.io/* internal keys always take precedence over user-supplied ones.
			Annotations: k.buildPodAnnotations(map[string]string{
				"olake.io/created-by-pod": k.config.WorkerIdentity,
				"olake.io/created-at":     time.Now().Format(time.RFC3339),
				"olake.io/workflow-id":    req.WorkflowID,
				"olake.io/operation-type": string(req.Command),
				"olake.io/connector-type": req.ConnectorType,
				"olake.io/job-id":         fmt.Sprintf("%d", req.JobID),
			}),
		},
		Spec: corev1.PodSpec{
			RestartPolicy:   corev1.RestartPolicyNever,
			NodeSelector:    k.GetNodeSelectorForJob(req.JobID, req.Command),
			Tolerations:     k.GetTolerationsForJob(req.JobID, req.Command),
			Affinity:        k.BuildAffinityForJob(req.JobID, req.Command),
			SecurityContext: k.config.SecurityContext,
			Containers: []corev1.Container{
				{
					Name:    "connector",
					Image:   imageName,
					Command: []string{},
					Args:    req.Args,
					// FallbackToLogsOnError makes Kubernetes write the last bytes of
					// stdout/stderr into ContainerStateTerminated.Message when the
					// process exits without writing to /dev/termination-log.
					// This is the only way to capture any context from an OOMKilled
					// container since the kernel kills the process before it can log.
					TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "job-storage",
							MountPath: "/mnt/config",
							SubPath:   subDir,
						},
					},
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU: k.parseQuantity(func() string {
								if k.config.JobPodCPURequest != "" {
									return k.config.JobPodCPURequest
								}
								return "100m"
							}()),
							corev1.ResourceMemory: k.parseQuantity(func() string {
								if k.config.JobPodMemoryRequest != "" {
									return k.config.JobPodMemoryRequest
								}
								return "256Mi"
							}()),
						},
						// No limits for flexibility
					},
					Env: []corev1.EnvVar{
						{
							Name:  "OLAKE_WORKFLOW_ID",
							Value: req.WorkflowID,
						},
						{
							Name:  "OLAKE_SECRET_KEY",
							Value: k.config.SecretKey,
						},
						{
							Name:  "JAVA_TOOL_OPTIONS",
							Value: k.getJavaHeapOptions(),
						},
					},
					EnvFrom: []corev1.EnvFromSource{
						{
							ConfigMapRef: &corev1.ConfigMapEnvSource{
								LocalObjectReference: corev1.LocalObjectReference{
									Name: "olake-global-env",
								},
								Optional: ptr.To(true),
							},
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "job-storage",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: k.config.PVCName,
						},
					},
				},
			},
		},
	}

	// Hand the drivers-project sentry DSN to the connector as SENTRY_DSN so
	// connector errors report to the drivers project, not the worker's.
	if dsn := os.Getenv("SENTRY_DSN_DRIVERS"); dsn != "" {
		pod.Spec.Containers[0].Env = append(pod.Spec.Containers[0].Env,
			corev1.EnvVar{Name: "SENTRY_DSN", Value: dsn})
	}

	// Set ServiceAccountName only if configured (non-empty)
	// If empty, Kubernetes will use the namespace's default service account
	if k.config.JobServiceAccount != "" && k.config.JobServiceAccount != "default" {
		pod.Spec.ServiceAccountName = k.config.JobServiceAccount
	}

	// Add liveness probe for long-running sync operations
	if slices.Contains(constants.AsyncCommands, req.Command) {
		pod.Spec.Containers[0].LivenessProbe = &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				Exec: &corev1.ExecAction{
					Command: []string{
						"/bin/sh",
						"-c",
						"echo ok > /mnt/config/.healthcheck",
					},
				},
			},
			InitialDelaySeconds: 10,
			PeriodSeconds:       30,
			TimeoutSeconds:      5,
			FailureThreshold:    3,
			SuccessThreshold:    1,
		}
	}

	return pod
}

func (k *KubernetesExecutor) createPod(ctx context.Context, podSpec *corev1.Pod) (*corev1.Pod, error) {
	log := logger.Log(ctx)
	result, err := k.client.CoreV1().Pods(k.namespace).Create(ctx, podSpec, metav1.CreateOptions{})
	if err != nil {
		if !apierrors.IsAlreadyExists(err) {
			log.Error("failed to create pod", "podName", podSpec.Name, "error", err)
			return nil, fmt.Errorf("failed to create pod: %s", err)
		}

		log.Info("pod already exists, resuming polling", "podName", podSpec.Name)

		// Fetch the existing pod
		existing, getErr := k.client.CoreV1().Pods(k.namespace).Get(ctx, podSpec.Name, metav1.GetOptions{})
		if getErr != nil {
			log.Error("pod exists but failed to fetch", "podName", podSpec.Name, "error", getErr)
			return nil, fmt.Errorf("pod exists but failed to fetch: %s", getErr)
		}
		return existing, nil
	}

	log.Info("successfully created pod", "podName", podSpec.Name)
	return result, nil
}
