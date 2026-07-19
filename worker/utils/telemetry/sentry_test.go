package telemetry

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScrubString(t *testing.T) {
	require.Equal(t,
		"postgres://***@db:5432/olake",
		scrubString("postgres://admin:s3cret@db:5432/olake"))
	require.Equal(t,
		"key AKIA**************** rejected",
		scrubString("key AKIAIOSFODNN7EXAMPLE rejected"))
	require.Equal(t,
		"failed query: [redacted]\nnext line",
		scrubString("failed query: SELECT * FROM users WHERE ssn='x'\nnext line"))
	require.Equal(t,
		"Statement: [redacted]",
		scrubString("Statement: INSERT INTO t VALUES (1)"))
	require.Equal(t, "plain error", scrubString("plain error"))
}
