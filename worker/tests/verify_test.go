package tests

import (
	"testing"

	_ "github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go"
)

func TestDinDIntegration(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	t.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	err := DinDTestContainer(t)
	if err != nil {
		t.Errorf("Error in Docker in Docker container start up: %s", err)
	}
}
