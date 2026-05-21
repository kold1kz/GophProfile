package config

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config хранит все настройки, которые нужны серверу и воркеру для старта.
// Значения приходят из .env, переменных окружения или из дефолтов ниже.
type Config struct {
	HTTPAddr          string
	WorkerMetricsAddr string
	// PublicBaseURL задает внешний адрес MinIO для presigned URL, например http://localhost:9000.
	PublicBaseURL  string
	DatabaseURL    string
	S3Endpoint     string
	S3AccessKey    string
	S3SecretKey    string
	S3Bucket       string
	S3UseSSL       bool
	RabbitURL      string
	RabbitExchange string
	RabbitQueue    string
	OTLPEndpoint   string
	MaxFileSize    int64
	ShutdownDelay  time.Duration
}

// LoadE собирает конфигурацию приложения и возвращает ошибку, если не хватает обязательных секретов.
func LoadE() (Config, error) {
	loadDotEnv(".env")

	databaseURL := getenv("DATABASE_URL", "")
	if databaseURL == "" {
		databaseURL = getenv("DATABASE_DSN", "")
	}
	if databaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL or DATABASE_DSN is required")
	}

	s3Endpoint := getenv("S3_ENDPOINT", "")
	s3AccessKey := getenv("S3_ACCESS_KEY", "")
	s3SecretKey := getenv("S3_SECRET_KEY", "")
	rabbitURL := getenv("RABBITMQ_URL", "")
	required := []struct {
		key   string
		value string
	}{
		{"S3_ENDPOINT", s3Endpoint},
		{"S3_ACCESS_KEY", s3AccessKey},
		{"S3_SECRET_KEY", s3SecretKey},
		{"RABBITMQ_URL", rabbitURL},
	}
	for _, item := range required {
		if item.value == "" {
			return Config{}, fmt.Errorf("%s is required", item.key)
		}
	}

	return Config{
		HTTPAddr:          getenv("HTTP_ADDR", ":8080"),
		WorkerMetricsAddr: getenv("WORKER_METRICS_ADDR", ":8081"),
		PublicBaseURL:     getenv("PUBLIC_BASE_URL", ""),
		DatabaseURL:       databaseURL,
		S3Endpoint:        s3Endpoint,
		S3AccessKey:       s3AccessKey,
		S3SecretKey:       s3SecretKey,
		S3Bucket:          getenv("S3_BUCKET", "avatars"),
		S3UseSSL:          getenvBool("S3_USE_SSL", false),
		RabbitURL:         rabbitURL,
		RabbitExchange:    getenv("RABBITMQ_EXCHANGE", "avatars.exchange"),
		RabbitQueue:       getenv("RABBITMQ_QUEUE", "avatars.worker"),
		OTLPEndpoint:      getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317"),
		MaxFileSize:       getenvInt64("MAX_FILE_SIZE", 10<<20),
		ShutdownDelay:     10 * time.Second,
	}, nil
}

func loadDotEnv(path string) {
	file, err := os.Open(path)
	if err != nil {
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		key, value, ok := parseDotEnvLine(scanner.Text())
		if !ok {
			continue
		}
		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		_ = os.Setenv(key, value)
	}
}

func parseDotEnvLine(line string) (string, string, bool) {
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(line, "#") {
		return "", "", false
	}
	line = strings.TrimPrefix(line, "export ")

	key, value, found := strings.Cut(line, "=")
	if !found {
		return "", "", false
	}

	key = strings.TrimSpace(key)
	if key == "" {
		return "", "", false
	}

	value = strings.TrimSpace(value)
	if len(value) >= 2 {
		quote := value[0]
		if (quote == '"' || quote == '\'') && value[len(value)-1] == quote {
			return key, value[1 : len(value)-1], true
		}
	}

	if comment := strings.Index(value, " #"); comment >= 0 {
		value = strings.TrimSpace(value[:comment])
	}

	return key, value, true
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getenvBool(key string, fallback bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func getenvInt64(key string, fallback int64) int64 {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}
