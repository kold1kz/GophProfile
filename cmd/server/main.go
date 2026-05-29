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

	"gophprofile/internal/api"
	"gophprofile/internal/app"
	"gophprofile/internal/config"
	"gophprofile/internal/handlers"
	"gophprofile/internal/observability"
	"gophprofile/internal/repository"
	"gophprofile/internal/services"
	"gophprofile/internal/storage"
)

func main() {
	logger := observability.NewBootstrapLogger("gophprofile-server")
	slog.SetDefault(logger)
	cfg, err := config.LoadE()
	if err != nil {
		logger.Error("load config", "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	otelLogger, shutdownLogging, err := observability.InitOTelLogger(ctx, "gophprofile-server", cfg.OTLPEndpoint)
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
	shutdownTracing, err := observability.InitTracing(ctx, "gophprofile-server", cfg.OTLPEndpoint)
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

	repo := repository.NewPostgres(db)
	if err := initializeStorageUsage(ctx, repo, logger); err != nil {
		logger.Warn("initialize storage usage metric", "error", err)
	}
	observability.RegisterInfrastructureCollectors(
		func() float64 { return float64(repo.OpenConnections()) },
		func() float64 {
			depth, err := broker.QueueDepth()
			if err != nil {
				logger.Warn("inspect rabbitmq queue depth", "error", err)
				return 0
			}
			return float64(depth)
		},
	)
	service := services.NewAvatarService(repo, s3, broker, cfg.MaxFileSize)
	go runOutboxPublisher(ctx, service, logger)
	handler := handlers.NewAvatarHandler(
		service,
		api.Health{DB: repo, S3: s3, Broker: broker},
		cfg.MaxFileSize,
		handlers.WithRateLimit(handlers.RateLimitConfig{
			RequestsPerSecond: cfg.RateLimitRPS,
			Burst:             cfg.RateLimitBurst,
		}),
	)

	server := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: handler.Routes(),
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("server listening", "addr", cfg.HTTPAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		logger.Error("listen", "error", err)
		stop()
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownDelay)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown", "error", err)
	}
}

func initializeStorageUsage(ctx context.Context, repo *repository.PostgresRepository, logger *slog.Logger) error {
	usage, err := repo.StorageUsageByUser(ctx)
	if err != nil {
		return err
	}
	for userID, bytes := range usage {
		observability.SetStorageUsage(userID, bytes)
		logger.Info("storage usage metric initialized", "user_id", userID, "bytes", bytes)
	}
	return nil
}

func runOutboxPublisher(ctx context.Context, service *services.AvatarService, logger *slog.Logger) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := service.FlushOutbox(ctx, 50); err != nil {
				logger.Error("flush outbox", "error", err)
			}
		}
	}
}
