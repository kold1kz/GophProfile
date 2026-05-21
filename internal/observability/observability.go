package observability

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otellogglobal "go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "gophprofile"

var (
	httpRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gophprofile_http_requests_total",
		Help: "Total number of HTTP requests.",
	}, []string{"method", "route", "status"})
	httpRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gophprofile_http_request_duration_seconds",
		Help:    "HTTP request duration in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "route", "status"})
	httpErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gophprofile_http_errors_total",
		Help: "Total number of HTTP 5xx responses.",
	}, []string{"method", "route"})

	uploadsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "avatars_uploads_total",
		Help: "Total number of avatar uploads.",
	}, []string{"status", "user_id"})
	uploadDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "avatars_upload_duration_seconds",
		Help:    "Avatar upload duration in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"status"})
	deletesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "avatars_deletes_total",
		Help: "Total number of avatar deletions.",
	}, []string{"status"})
	storageUsage = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "avatars_storage_bytes",
		Help: "Total storage used by avatars.",
	}, []string{"user_id"})
	workerEventsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "avatar_worker_events_total",
		Help: "Total number of worker events.",
	}, []string{"event", "status"})
)

// InitOTelLogger configures process-wide slog output backed by OpenTelemetry logs.
func InitOTelLogger(ctx context.Context, service, endpoint string) (*slog.Logger, func(context.Context) error, error) {
	if endpoint == "" {
		logger := NewBootstrapLogger(service)
		slog.SetDefault(logger)
		return logger, func(context.Context) error { return nil }, nil
	}

	exporter, err := otlploggrpc.New(ctx, otlploggrpc.WithEndpoint(endpoint), otlploggrpc.WithInsecure())
	if err != nil {
		return nil, nil, err
	}
	provider := sdklog.NewLoggerProvider(
		sdklog.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(service),
		)),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)),
	)
	otellogglobal.SetLoggerProvider(provider)

	logger := slog.New(otelslog.NewHandler(
		service,
		otelslog.WithLoggerProvider(provider),
		otelslog.WithSource(true),
	))
	slog.SetDefault(logger)
	return logger, provider.Shutdown, nil
}

// NewBootstrapLogger returns a local logger used before telemetry configuration is loaded.
func NewBootstrapLogger(service string) *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})).With("service", service)
}

// InitTracing configures OpenTelemetry tracing and returns a shutdown function.
func InitTracing(ctx context.Context, service, endpoint string) (func(context.Context) error, error) {
	if endpoint == "" {
		otel.SetTracerProvider(trace.NewNoopTracerProvider())
		otel.SetTextMapPropagator(propagation.TraceContext{})
		return func(context.Context) error { return nil }, nil
	}

	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(endpoint), otlptracegrpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(service),
		)),
	)
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	return provider.Shutdown, nil
}

// Tracer returns the project tracer.
func Tracer() trace.Tracer {
	return otel.Tracer(tracerName)
}

// LogAttrs returns trace correlation attributes for slog.
func LogAttrs(ctx context.Context) []slog.Attr {
	spanContext := trace.SpanFromContext(ctx).SpanContext()
	if !spanContext.IsValid() {
		return nil
	}
	return []slog.Attr{
		slog.String("trace_id", spanContext.TraceID().String()),
		slog.String("span_id", spanContext.SpanID().String()),
	}
}

// HTTPMiddleware records RED metrics. HTTP spans are created by otelhttp.
func HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(recorder, r)

		route := chi.RouteContext(r.Context()).RoutePattern()
		if route == "" {
			route = r.URL.Path
		}
		status := strconv.Itoa(recorder.status)
		duration := time.Since(start).Seconds()
		httpRequestsTotal.WithLabelValues(r.Method, route, status).Inc()
		httpRequestDuration.WithLabelValues(r.Method, route, status).Observe(duration)
		if recorder.status >= 500 {
			httpErrorsTotal.WithLabelValues(r.Method, route).Inc()
		}
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func ObserveUpload(userID, status string, duration time.Duration) {
	uploadsTotal.WithLabelValues(status, userID).Inc()
	uploadDuration.WithLabelValues(status).Observe(duration.Seconds())
}

func ObserveDelete(status string) {
	deletesTotal.WithLabelValues(status).Inc()
}

func AddStorageUsage(userID string, delta int64) {
	storageUsage.WithLabelValues(userID).Add(float64(delta))
}

func SetStorageUsage(userID string, value int64) {
	storageUsage.WithLabelValues(userID).Set(float64(value))
}

func ObserveWorkerEvent(event, status string) {
	workerEventsTotal.WithLabelValues(event, status).Inc()
}

func RegisterInfrastructureCollectors(openDBConnections func() float64, queueDepth func() float64) {
	prometheus.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "gophprofile_db_open_connections",
		Help: "Current number of open PostgreSQL connections.",
	}, openDBConnections))
	prometheus.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "gophprofile_queue_depth",
		Help: "Current number of messages ready in the avatar worker queue.",
	}, queueDepth))
}
