# GophProfile

MVP сервиса аватарок на Go: REST API, Chi, PostgreSQL, MinIO S3, RabbitMQ worker и простой веб-интерфейс.

## Запуск

```bash
docker compose up --build
```

После запуска:

- API: `http://localhost:8080/api/v1`
- Web upload: `http://localhost:8080/web/upload`
- Health: `http://localhost:8080/health`
- Metrics: `http://localhost:8080/metrics`
- OpenTelemetry Collector health: `http://localhost:13133`
- PostgreSQL: `localhost:15432`
- RabbitMQ UI: `http://localhost:15672` (`guest` / `guest`)
- MinIO UI: `http://localhost:9001` (`admin` / `adminadmin`)
- Prometheus: `http://localhost:9090`
- Jaeger: `http://localhost:16686`
- Grafana: `http://localhost:3000` (`admin` / `admin`)
- Loki: `http://localhost:3100`

Трейсы и структурированные `slog`-логи отправляются из `server` и `worker` по OTLP/gRPC в OpenTelemetry Collector (`otel-collector:4317`). Collector экспортирует трейсы в Jaeger, а логи в Loki.

## Основные эндпоинты

```bash
curl -F file=@avatar.jpg -H 'X-User-ID: user-1' http://localhost:8080/api/v1/avatars
curl http://localhost:8080/api/v1/users/user-1/avatars
curl http://localhost:8080/api/v1/users/user-1/avatar --output avatar.jpg
curl -X DELETE -H 'X-User-ID: user-1' http://localhost:8080/api/v1/avatars/<avatar_id>
```
