package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoadReadsDotEnvFile(t *testing.T) {
	chdir(t, t.TempDir())
	writeDotEnv(t, `
HTTP_ADDR=:9090
PUBLIC_BASE_URL="https://cdn.example.test"
DATABASE_URL=postgres://env:env@localhost:5432/envdb?sslmode=disable
S3_ENDPOINT=minio:9000
S3_ACCESS_KEY=access
S3_SECRET_KEY='secret'
S3_BUCKET=profile-avatars
S3_USE_SSL=true
RABBITMQ_URL=amqp://user:pass@rabbit:5672/
RABBITMQ_EXCHANGE=profile.exchange
RABBITMQ_QUEUE=profile.worker
MAX_FILE_SIZE=2048
SHUTDOWN_DELAY=3s
`)
	unsetConfigEnv(t)

	cfg, err := LoadE()
	require.NoError(t, err)

	require.Equal(t, ":9090", cfg.HTTPAddr)
	require.Equal(t, "https://cdn.example.test", cfg.PublicBaseURL)
	require.Equal(t, "postgres://env:env@localhost:5432/envdb?sslmode=disable", cfg.DatabaseURL)
	require.Equal(t, "minio:9000", cfg.S3Endpoint)
	require.Equal(t, "access", cfg.S3AccessKey)
	require.Equal(t, "secret", cfg.S3SecretKey)
	require.Equal(t, "profile-avatars", cfg.S3Bucket)
	require.True(t, cfg.S3UseSSL)
	require.Equal(t, "amqp://user:pass@rabbit:5672/", cfg.RabbitURL)
	require.Equal(t, "profile.exchange", cfg.RabbitExchange)
	require.Equal(t, "profile.worker", cfg.RabbitQueue)
	require.EqualValues(t, 2048, cfg.MaxFileSize)
	require.Equal(t, 3*time.Second, cfg.ShutdownDelay)
}

func TestLoadDoesNotOverrideExistingEnvironment(t *testing.T) {
	chdir(t, t.TempDir())
	writeDotEnv(t, requiredEnv()+"HTTP_ADDR=:9090\nDATABASE_URL=postgres://from-file\n")
	unsetConfigEnv(t)
	t.Setenv("HTTP_ADDR", ":7070")

	cfg, err := LoadE()
	require.NoError(t, err)

	require.Equal(t, ":7070", cfg.HTTPAddr)
	require.Equal(t, "postgres://from-file", cfg.DatabaseURL)
}

func TestLoadFallsBackToDatabaseDSN(t *testing.T) {
	chdir(t, t.TempDir())
	writeDotEnv(t, requiredEnv()+"DATABASE_DSN=postgres://fallback\n")
	unsetConfigEnv(t)

	cfg, err := LoadE()
	require.NoError(t, err)

	require.Equal(t, "postgres://fallback", cfg.DatabaseURL)
}

func TestLoadAllowsEmptyOTLPEndpoint(t *testing.T) {
	chdir(t, t.TempDir())
	writeDotEnv(t, requiredEnv()+"DATABASE_URL=postgres://db\n")
	unsetConfigEnv(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	cfg, err := LoadE()
	require.NoError(t, err)

	require.Empty(t, cfg.OTLPEndpoint)
}

func TestLoadReturnsErrorForInvalidShutdownDelay(t *testing.T) {
	chdir(t, t.TempDir())
	writeDotEnv(t, requiredEnv()+"DATABASE_URL=postgres://db\nSHUTDOWN_DELAY=soon\n")
	unsetConfigEnv(t)

	_, err := LoadE()

	require.Error(t, err)
	require.Contains(t, err.Error(), "SHUTDOWN_DELAY")
}

func TestLoadEReturnsErrorWhenRequiredSecretMissing(t *testing.T) {
	chdir(t, t.TempDir())
	writeDotEnv(t, "DATABASE_URL=postgres://db\nS3_ENDPOINT=minio:9000\nS3_ACCESS_KEY=access\n")
	unsetConfigEnv(t)

	_, err := LoadE()

	require.Error(t, err)
	require.Contains(t, err.Error(), "S3_SECRET_KEY")
}

func TestParseDotEnvLine(t *testing.T) {
	tests := []struct {
		name      string
		line      string
		wantKey   string
		wantValue string
		wantOK    bool
	}{
		{name: "empty", line: "", wantOK: false},
		{name: "comment", line: "# comment", wantOK: false},
		{name: "missing equals", line: "HTTP_ADDR", wantOK: false},
		{name: "plain", line: "HTTP_ADDR=:8081", wantKey: "HTTP_ADDR", wantValue: ":8081", wantOK: true},
		{name: "export", line: "export S3_BUCKET=avatars", wantKey: "S3_BUCKET", wantValue: "avatars", wantOK: true},
		{name: "double quotes", line: `PUBLIC_BASE_URL="http://localhost:8080"`, wantKey: "PUBLIC_BASE_URL", wantValue: "http://localhost:8080", wantOK: true},
		{name: "single quotes", line: `S3_SECRET_KEY='secret value'`, wantKey: "S3_SECRET_KEY", wantValue: "secret value", wantOK: true},
		{name: "inline comment", line: "MAX_FILE_SIZE=1024 # bytes", wantKey: "MAX_FILE_SIZE", wantValue: "1024", wantOK: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key, value, ok := parseDotEnvLine(tt.line)
			require.Equal(t, tt.wantOK, ok)
			require.Equal(t, tt.wantKey, key)
			require.Equal(t, tt.wantValue, value)
		})
	}
}

func writeDotEnv(t *testing.T, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(".", ".env"), []byte(content), 0o600))
}

func requiredEnv() string {
	return `
S3_ENDPOINT=minio:9000
S3_ACCESS_KEY=access
S3_SECRET_KEY=secret
RABBITMQ_URL=amqp://user:pass@rabbit:5672/
`
}

func chdir(t *testing.T, dir string) {
	t.Helper()
	previous, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(dir))
	t.Cleanup(func() {
		require.NoError(t, os.Chdir(previous))
	})
}

func unsetConfigEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"HTTP_ADDR",
		"PUBLIC_BASE_URL",
		"DATABASE_URL",
		"DATABASE_DSN",
		"S3_ENDPOINT",
		"S3_ACCESS_KEY",
		"S3_SECRET_KEY",
		"S3_BUCKET",
		"S3_USE_SSL",
		"RABBITMQ_URL",
		"RABBITMQ_EXCHANGE",
		"RABBITMQ_QUEUE",
		"OTEL_EXPORTER_OTLP_ENDPOINT",
		"MAX_FILE_SIZE",
		"SHUTDOWN_DELAY",
	} {
		t.Setenv(key, "")
		require.NoError(t, os.Unsetenv(key))
	}
}
