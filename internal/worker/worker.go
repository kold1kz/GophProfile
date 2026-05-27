package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"gophprofile/internal/domain"
	"gophprofile/internal/observability"
	"gophprofile/internal/queue"
	"gophprofile/internal/services"
	"gophprofile/pkg/imaging"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// Worker читает события из RabbitMQ и выполняет тяжелую фоновую работу с файлами.
type Worker struct {
	repo    services.AvatarRepository
	storage services.ObjectStorage
	events  eventConsumer
}

type eventConsumer interface {
	Consume() (<-chan amqp.Delivery, error)
}

var (
	decodeImage = imaging.Decode
	resizeJPEG  = imaging.ResizeJPEG
)

// New создает worker с репозиторием, объектным хранилищем и очередью событий.
func New(repo services.AvatarRepository, storage services.ObjectStorage, events *queue.RabbitMQ) *Worker {
	if events == nil {
		return &Worker{repo: repo, storage: storage}
	}
	return &Worker{repo: repo, storage: storage, events: events}
}

// Run запускает цикл чтения сообщений и останавливается, когда закрыт контекст или очередь.
func (w *Worker) Run(ctx context.Context) error {
	for {
		deliveries, err := w.events.Consume()
		if err != nil {
			timer := time.NewTimer(time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
				continue
			}
		}

		if err := w.consume(ctx, deliveries); err != nil {
			return err
		}
	}
}

func (w *Worker) consume(ctx context.Context, deliveries <-chan amqp.Delivery) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case delivery, ok := <-deliveries:
			if !ok {
				return nil
			}
			w.handleDelivery(ctx, delivery)
		}
	}
}

func (w *Worker) handleDelivery(ctx context.Context, delivery amqp.Delivery) {
	ctx = otel.GetTextMapPropagator().Extract(ctx, queueHeadersCarrier(delivery.Headers))
	ctx, span := observability.Tracer().Start(ctx, "worker.handle_delivery")
	defer span.End()
	span.SetAttributes(attribute.String("routing_key", delivery.RoutingKey), attribute.String("message_id", delivery.MessageId))
	status := "error"
	err := retry(ctx, 3, func() error {
		switch delivery.RoutingKey {
		case queue.RoutingAvatarUploaded:
			var event domain.AvatarUploadEvent
			if err := json.Unmarshal(delivery.Body, &event); err != nil {
				return err
			}
			return w.HandleUploadEvent(ctx, event)
		case queue.RoutingAvatarDeleted:
			var event domain.AvatarDeleteEvent
			if err := json.Unmarshal(delivery.Body, &event); err != nil {
				return err
			}
			return w.HandleDeleteEvent(ctx, event)
		default:
			return fmt.Errorf("unknown routing key: %s", delivery.RoutingKey)
		}
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		observability.ObserveWorkerEvent(delivery.RoutingKey, status)
		_ = delivery.Nack(false, false)
		return
	}
	status = "success"
	observability.ObserveWorkerEvent(delivery.RoutingKey, status)
	_ = delivery.Ack(false)
}

// HandleUploadEvent обрабатывает загрузку: делает миниатюры, сохраняет их и обновляет статус аватара.
func (w *Worker) HandleUploadEvent(ctx context.Context, event domain.AvatarUploadEvent) error {
	ctx, span := observability.Tracer().Start(ctx, "worker.handle_upload_event")
	defer span.End()
	span.SetAttributes(attribute.String("avatar_id", event.AvatarID), attribute.String("user_id", event.UserID), attribute.String("message_id", event.MessageID))
	if event.MessageID != "" {
		processed, err := w.repo.ProcessedMessage(ctx, event.MessageID)
		if err != nil {
			return err
		}
		if processed {
			return nil
		}
	}

	locked, err := w.repo.MarkProcessing(ctx, event.AvatarID)
	if err != nil {
		return err
	}
	if !locked {
		if event.MessageID != "" {
			return w.repo.SaveProcessedMessage(ctx, event.MessageID)
		}
		return nil
	}

	body, _, err := w.storage.Download(ctx, event.S3Key)
	if err != nil {
		if markErr := w.repo.UpdateProcessingFailed(ctx, event.AvatarID); markErr != nil {
			return fmt.Errorf("download original: %w; mark failed: %v", err, markErr)
		}
		return err
	}
	defer body.Close()

	info, err := decodeImage(body)
	if err != nil {
		if markErr := w.repo.UpdateProcessingFailed(ctx, event.AvatarID); markErr != nil {
			return fmt.Errorf("decode original: %w; mark failed: %v", err, markErr)
		}
		return err
	}

	sizes := []struct {
		name   string
		width  int
		height int
	}{
		{name: "100x100", width: 100, height: 100},
		{name: "300x300", width: 300, height: 300},
	}
	thumbnails := make([]domain.Thumbnail, 0, len(sizes))
	for _, size := range sizes {
		data, err := resizeJPEG(info.Image, size.width, size.height)
		if err != nil {
			if markErr := w.repo.UpdateProcessingFailed(ctx, event.AvatarID); markErr != nil {
				return fmt.Errorf("resize thumbnail: %w; mark failed: %v", err, markErr)
			}
			return err
		}
		key := fmt.Sprintf("thumbnails/%s/%s.jpg", event.AvatarID, size.name)
		if err := w.storage.Upload(ctx, key, "image/jpeg", int64(len(data)), bytes.NewReader(data)); err != nil {
			if markErr := w.repo.UpdateProcessingFailed(ctx, event.AvatarID); markErr != nil {
				return fmt.Errorf("upload thumbnail: %w; mark failed: %v", err, markErr)
			}
			return err
		}
		url, err := w.storage.PresignedGetURL(ctx, key)
		if err != nil {
			if markErr := w.repo.UpdateProcessingFailed(ctx, event.AvatarID); markErr != nil {
				return fmt.Errorf("presign thumbnail: %w; mark failed: %v", err, markErr)
			}
			return err
		}
		thumbnails = append(thumbnails, domain.Thumbnail{Size: size.name, S3Key: key, URL: url})
	}
	if err := w.repo.UpdateProcessed(ctx, event.AvatarID, thumbnails); err != nil {
		return err
	}
	if event.MessageID != "" {
		return w.repo.SaveProcessedMessage(ctx, event.MessageID)
	}
	return nil
}

// HandleDeleteEvent удаляет из S3 все файлы, связанные с аватаром.
func (w *Worker) HandleDeleteEvent(ctx context.Context, event domain.AvatarDeleteEvent) error {
	ctx, span := observability.Tracer().Start(ctx, "worker.handle_delete_event")
	defer span.End()
	span.SetAttributes(attribute.String("avatar_id", event.AvatarID), attribute.String("message_id", event.MessageID), attribute.Int("s3_keys", len(event.S3Keys)))
	if event.MessageID != "" {
		processed, err := w.repo.ProcessedMessage(ctx, event.MessageID)
		if err != nil {
			return err
		}
		if processed {
			return nil
		}
	}
	for _, key := range event.S3Keys {
		if key == "" {
			continue
		}
		if err := w.storage.Delete(ctx, key); err != nil {
			return err
		}
	}
	if event.MessageID != "" {
		return w.repo.SaveProcessedMessage(ctx, event.MessageID)
	}
	return nil
}

type queueHeadersCarrier amqp.Table

func (c queueHeadersCarrier) Get(key string) string {
	value, ok := c[key]
	if !ok {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return fmt.Sprint(typed)
	}
}

func (c queueHeadersCarrier) Set(key, value string) {
	c[key] = value
}

func (c queueHeadersCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for key := range c {
		keys = append(keys, key)
	}
	return keys
}

func retry(ctx context.Context, attempts int, fn func() error) error {
	var err error
	delay := 300 * time.Millisecond
	for i := 0; i < attempts; i++ {
		if err = fn(); err == nil {
			return nil
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		delay *= 2
	}
	return err
}
