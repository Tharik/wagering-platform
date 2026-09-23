package postgres

import (
	"os"
	"testing"

	"github.com/Tharik/wagering-platform/internal/testsupport"
)

func TestMain(m *testing.M) {
	os.Exit(testsupport.RunWithPostgresSchema("infrastructure_postgres", m.Run))
}
