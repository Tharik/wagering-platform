package testsupport

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const testDatabaseURL = "postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable"

// RunWithPostgresSchema runs one package's tests in a migrated, process-local schema.
func RunWithPostgresSchema(packageHint string, run func() int) int {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	schema := schemaName(packageHint)
	adminPool, err := pgxpool.New(ctx, testDatabaseURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "test postgres: connect: %v\n", err)
		return 1
	}
	defer adminPool.Close()

	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err := adminPool.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		fmt.Fprintf(os.Stderr, "test postgres: create schema %s: %v\n", schema, err)
		return 1
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if _, err := adminPool.Exec(cleanupCtx, "DROP SCHEMA IF EXISTS "+identifier+" CASCADE"); err != nil {
			fmt.Fprintf(os.Stderr, "test postgres: drop schema %s: %v\n", schema, err)
		}
	}()

	if err := applyMigrations(ctx, schema); err != nil {
		fmt.Fprintf(os.Stderr, "test postgres: migrate schema %s: %v\n", schema, err)
		return 1
	}

	restorePGOptions := setEnv("PGOPTIONS", appendPGOptions(os.Getenv("PGOPTIONS"), schema))
	defer restorePGOptions()
	restoreDatabaseURL := setEnv("DATABASE_URL", testDatabaseURL)
	defer restoreDatabaseURL()

	if err := verifySchema(ctx, schema); err != nil {
		fmt.Fprintf(os.Stderr, "test postgres: verify schema %s: %v\n", schema, err)
		return 1
	}
	fmt.Fprintf(os.Stderr, "test postgres: package %s using schema %s\n", packageHint, schema)

	return run()
}

func schemaName(packageHint string) string {
	var safe strings.Builder
	for _, char := range strings.ToLower(packageHint) {
		if char >= 'a' && char <= 'z' || char >= '0' && char <= '9' {
			safe.WriteRune(char)
		} else {
			safe.WriteByte('_')
		}
	}
	hint := safe.String()
	if len(hint) > 24 {
		hint = hint[:24]
	}
	unique := strings.ReplaceAll(uuid.NewString(), "-", "")[:16]
	return fmt.Sprintf("test_%s_%d_%s", hint, os.Getpid(), unique)
}

func applyMigrations(ctx context.Context, schema string) error {
	config, err := pgxpool.ParseConfig(testDatabaseURL)
	if err != nil {
		return err
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema + ",public"

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return err
	}
	defer pool.Close()

	files, err := filepath.Glob(filepath.Join(repositoryRoot(), "migrations", "*.up.sql"))
	if err != nil {
		return err
	}
	sort.Strings(files)
	if len(files) != 6 {
		return fmt.Errorf("expected 6 up migrations, found %d", len(files))
	}

	for _, file := range files {
		contents, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("read %s: %w", filepath.Base(file), err)
		}
		if _, err := pool.Exec(ctx, string(contents)); err != nil {
			return fmt.Errorf("apply %s: %w", filepath.Base(file), err)
		}
	}
	return nil
}

func verifySchema(ctx context.Context, schema string) error {
	pool, err := pgxpool.New(ctx, testDatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	var searchPath, currentSchema string
	var wallets *string
	if err := pool.QueryRow(ctx, `SELECT current_setting('search_path'), current_schema(), to_regclass('wallets')::text`).Scan(&searchPath, &currentSchema, &wallets); err != nil {
		return err
	}
	if currentSchema != schema || wallets == nil || *wallets != "wallets" {
		return fmt.Errorf("unexpected resolution: search_path=%q current_schema=%q wallets=%v", searchPath, currentSchema, wallets)
	}
	return nil
}

func repositoryRoot() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", ".."))
}

func appendPGOptions(previous, schema string) string {
	value := strings.TrimSpace(previous + " -c search_path=" + schema + ",public")
	return value
}

func setEnv(name, value string) func() {
	previous, existed := os.LookupEnv(name)
	_ = os.Setenv(name, value)
	return func() {
		if existed {
			_ = os.Setenv(name, previous)
			return
		}
		_ = os.Unsetenv(name)
	}
}
