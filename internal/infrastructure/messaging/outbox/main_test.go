package outbox

import (
	"os"
	"testing"

	"github.com/Tharik/wagering-platform/internal/testsupport"
)

func TestMain(m *testing.M) {
	os.Exit(testsupport.RunWithPostgresSchema("messaging_outbox", m.Run))
}
