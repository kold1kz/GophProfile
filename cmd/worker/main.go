package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gophprofile/internal/app"
	"gophprofile/internal/config"
	"gophprofile/internal/observability"
	"gophprofile/internal/repository"
	"gophprofile/internal/storage"
	avatarworker "gophprofile/internal/worker"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	logger := observability.NewBootstrapLogger("gophprofile-worker")
	slog.SetDefault(logger)
	cfg, err := config.LoadE()
	if err != nil {
		logger.Error("load config", "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	otelLogger, shutdownLogging, err := observability.InitOTelLogger(ctx, "gophprofile-worker", cfg.OTLPEndpoint)
	if err != nil {
		logger.Error("init logging", "error", err)
		os.Exit(1)
	}
	logger = otelLogger
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownLogging(shutdownCtx); err != nil {
			logger.Error("shutdown logging", "error", err)
		}
	}()
	shutdownTracing, err := observability.InitTracing(ctx, "gophprofile-worker", cfg.OTLPEndpoint)
	if err != nil {
		logger.Error("init tracing", "error", err)
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTracing(shutdownCtx); err != nil {
			logger.Error("shutdown tracing", "error", err)
		}
	}()

	db, err := repository.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("connect postgres", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	s3, err := storage.NewMinIO(ctx, cfg.S3Endpoint, cfg.PublicBaseURL, cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3Bucket, cfg.S3UseSSL)
	if err != nil {
		logger.Error("connect s3", "error", err)
		os.Exit(1)
	}

	broker, err := app.ConnectRabbit(ctx, cfg.RabbitURL, cfg.RabbitExchange, cfg.RabbitQueue)
	if err != nil {
		logger.Error("connect rabbitmq", "error", err)
		os.Exit(1)
	}
	defer broker.Close()

	w := avatarworker.New(repository.NewPostgres(db), s3, broker)
	metricsServer := startMetricsServer(cfg.WorkerMetricsAddr, logger)
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := metricsServer.Shutdown(shutdownCtx); err != nil {
			logger.Error("shutdown metrics server", "error", err)
		}
	}()
	logger.Info("worker started")
	if err := w.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("worker failed", "error", err)
		os.Exit(1)
	}
}

func startMetricsServer(addr string, logger *slog.Logger) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	server := &http.Server{Addr: addr, Handler: mux}
	go func() {
		logger.Info("worker metrics listening", "addr", addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("worker metrics listen", "error", err)
		}
	}()
	return server
}
