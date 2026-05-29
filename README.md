# GophProfile

MVP сервиса аватарок на Go: REST API, Chi, PostgreSQL, MinIO S3, RabbitMQ worker, OpenTelemetry, Prometheus metrics и простой веб-интерфейс.

## Локальный запуск через Docker Compose

```bash
docker compose up -d --build
```

После запуска:

- API: `http://localhost:8080/api/v1`
- Web upload: `http://localhost:8080/web/upload`
- Health: `http://localhost:8080/health`
- Liveness: `http://localhost:8080/live`
- Readiness: `http://localhost:8080/ready`
- Metrics: `http://localhost:8080/metrics`
- Worker metrics: `http://localhost:8081/metrics`
- Worker health: `http://localhost:8081/healthz`
- OpenTelemetry Collector health: `http://localhost:13133`
- PostgreSQL: `localhost:15432`
- RabbitMQ UI: `http://localhost:15672` (`guest` / `guest`)
- MinIO UI: `http://localhost:9001` (`admin` / `adminadmin`)
- Prometheus: `http://localhost:9090`
- Jaeger: `http://localhost:16686`
- Grafana: `http://localhost:3000` (`admin` / `admin`)
- Loki: `http://localhost:3100`

Трейсы и структурированные `slog`-логи отправляются из `server` и `worker` по OTLP/gRPC в OpenTelemetry Collector. Collector экспортирует трейсы в Jaeger, а логи в Loki.

## Основные эндпоинты

```bash
curl -F file=@avatar.jpg -H 'X-User-ID: user-1' http://localhost:8080/api/v1/avatars
curl http://localhost:8080/api/v1/users/user-1/avatars
curl http://localhost:8080/api/v1/users/user-1/avatar --output avatar.jpg
curl -X DELETE -H 'X-User-ID: user-1' http://localhost:8080/api/v1/avatars/<avatar_id>
```

OpenAPI спецификация: [docs/openapi.yaml](docs/openapi.yaml).

## Kubernetes и Helm

Helm chart находится в [charts/gophprofile](charts/gophprofile). Он содержит:

- `Deployment` для `server` и `worker`;
- `Service` для HTTP API и worker metrics;
- `Ingress` для внешнего HTTP-трафика;
- `ConfigMap` и `Secret` для конфигурации;
- `HorizontalPodAutoscaler` по CPU и memory;
- `ServiceMonitor` для Prometheus Operator;
- `NetworkPolicy`, `ServiceAccount`, `Role`, `RoleBinding` и `SecurityContext`;
- Helm hook для применения SQL-миграций: `pre-install,pre-upgrade` в production с внешней БД и `post-install,pre-upgrade` в локальном стенде, где PostgreSQL поднимается этим же chart.

Архитектурная схема: [docs/architecture.md](docs/architecture.md).

### Локальный кластер Rancher Desktop

Локальный values-файл поднимает не только приложение, но и dev-инфраструктуру в Kubernetes: PostgreSQL, RabbitMQ и MinIO. Это удобно для Rancher Desktop и учебного стенда; для production используйте внешние managed-сервисы и `values-production.yaml`.

Соберите Docker image:

```bash
docker build -t gophprofile-server:latest .
```

Подготовьте namespace:

```bash
kubectl create namespace gophprofile
```

Установите chart:

```bash
helm upgrade --install gophprofile ./charts/gophprofile \
  --namespace gophprofile \
  -f charts/gophprofile/values-local.yaml
```

Для локального Ingress добавьте запись в `/etc/hosts`:

```text
127.0.0.1 gophprofile.local
```

Проверка:

```bash
kubectl -n gophprofile get pods,svc,ingress,hpa
curl http://gophprofile.local/live
curl http://gophprofile.local/ready
```

Если Ingress Controller в локальном кластере не установлен, можно проверить сервис через port-forward:

```bash
kubectl -n gophprofile port-forward svc/gophprofile-server 8080:80
curl http://localhost:8080/health
```

### Production-like деплой

В `values.yaml` секреты по умолчанию пустые, чтобы сервис не стартовал с публично известными паролями. Для production не храните секреты в values-файлах и создайте Secret заранее:

```bash
kubectl -n gophprofile create secret generic gophprofile-secrets \
  --from-literal=DATABASE_URL='postgres://user:pass@postgres:5432/gophprofile?sslmode=require' \
  --from-literal=S3_ACCESS_KEY='change-me' \
  --from-literal=S3_SECRET_KEY='change-me' \
  --from-literal=RABBITMQ_URL='amqp://user:pass@rabbitmq:5672/'
```

Затем установите chart:

```bash
helm upgrade --install gophprofile ./charts/gophprofile \
  --namespace gophprofile \
  --create-namespace \
  -f charts/gophprofile/values-production.yaml \
  --set image.repository=registry.example.com/gophprofile/server \
  --set image.tag=1.0.0
```

Для Prometheus Operator оставьте `serviceMonitor.enabled=true`. Если в кластере нет CRD `monitoring.coreos.com/v1`, отключите ServiceMonitor:

```bash
--set serviceMonitor.enabled=false
```

Для кластеров без NetworkPolicy controller можно отключить политики:

```bash
--set networkPolicy.enabled=false
```

Если зависимостям нужны egress-правила не по namespace labels, задайте конкретные CIDR через `networkPolicy.egressIPBlocks`; chart не открывает fallback на `0.0.0.0/0`.

## Graceful Shutdown

`server` обрабатывает `SIGTERM` через `signal.NotifyContext`, завершает HTTP server через `Shutdown` и использует настраиваемый `SHUTDOWN_DELAY` с дефолтом `10s`. RabbitMQ reconnect loop останавливается через `Close`, поэтому shutdown не блокируется долгим backoff sleep.

## Production readiness

- Liveness probe использует `/live` и проверяет только процесс HTTP-сервера.
- Readiness probe использует `/ready` и проверяет PostgreSQL, MinIO и RabbitMQ.
- Worker probes используют `/healthz` на metrics-порту и проверяют PostgreSQL, MinIO и RabbitMQ; `/metrics` остается только endpoint для Prometheus.
- HTTP rate limiting включен на уровне middleware. Настройки: `RATE_LIMIT_RPS` и `RATE_LIMIT_BURST`.
- Circuit breaker включен для PostgreSQL, MinIO и RabbitMQ. После серии ошибок breaker временно прекращает обращения к проблемной зависимости и возвращает ошибку `circuit breaker is open`.
- Контейнер запускается non-root пользователем `10001`, а Kubernetes chart дополнительно задает `runAsNonRoot`, `readOnlyRootFilesystem`, `seccompProfile` и сброс Linux capabilities.

## Мониторинг и алерты

Prometheus собирает `/metrics` у `server` и `worker` через `ServiceMonitor`. В Docker Compose Prometheus использует [docker/prometheus/prometheus.yml](docker/prometheus/prometheus.yml), а в Kubernetes discovery выполняет Prometheus Operator.

Grafana dashboard [docker/grafana/dashboards/gophprofile-overview.json](docker/grafana/dashboards/gophprofile-overview.json) показывает request rate, error rate, p95 latency, загрузки аватаров, глубину RabbitMQ очереди, использование storage и runtime-метрики процесса. Для состояния Kubernetes-кластера используйте стандартные dashboards Prometheus Operator/kube-prometheus-stack: pod restarts, CPU/memory requests/limits, HPA status и node health.

Алерты описаны в [docker/prometheus/alerts.yml](docker/prometheus/alerts.yml):

- `HighUploadErrorRate`: доля ошибок загрузки аватаров выше 10%.
- `HighUploadResponseTime`: p95 загрузки выше 5 секунд.
- `HighHTTPErrorRate`: доля 5xx выше 5%.
- `InstanceDown`: Prometheus не может scrape-ить server metrics.
- `HighQueueDepth`: очередь worker содержит больше 100 сообщений.
