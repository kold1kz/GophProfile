package services

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"time"

	"gophprofile/internal/domain"
	"gophprofile/internal/observability"
	"gophprofile/pkg/imaging"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

var (
	// ErrFileTooLarge означает, что загруженный файл превысил лимит из конфигурации.
	ErrFileTooLarge = errors.New("file too large")
)

// AvatarService содержит основную бизнес-логику аватаров.
// Он связывает в один сценарий репозиторий, объектное хранилище и очередь событий.
type AvatarService struct {
	repo        AvatarRepository
	storage     ObjectStorage
	publisher   EventPublisher
	maxFileSize int64
}

// NewAvatarService создает сервис аватаров с нужными зависимостями.
func NewAvatarService(repo AvatarRepository, storage ObjectStorage, publisher EventPublisher, maxFileSize int64) *AvatarService {
	return &AvatarService{repo: repo, storage: storage, publisher: publisher, maxFileSize: maxFileSize}
}

// UploadInput собирает данные, которые приходят при загрузке файла.
type UploadInput struct {
	UserID   string
	FileName string
	Size     int64
	Reader   io.Reader
}

// Upload проверяет файл, сохраняет оригинал в S3, создает запись в базе и публикует событие для worker.
func (s *AvatarService) Upload(ctx context.Context, in UploadInput) (domain.Avatar, error) {
	start := time.Now()
	ctx, span := observability.Tracer().Start(ctx, "service.upload_avatar")
	defer span.End()
	span.SetAttributes(
		attribute.String("user_id", in.UserID),
		attribute.String("file_name", in.FileName),
		attribute.Int64("file_size", in.Size),
	)
	status := "error"
	defer func() {
		observability.ObserveUpload(status, time.Since(start))
	}()

	if strings.TrimSpace(in.UserID) == "" {
		span.SetStatus(codes.Error, "user id is required")
		return domain.Avatar{}, errors.New("user id is required")
	}
	if in.Size > s.maxFileSize {
		span.SetStatus(codes.Error, ErrFileTooLarge.Error())
		return domain.Avatar{}, ErrFileTooLarge
	}

	limited := io.LimitReader(in.Reader, s.maxFileSize+1)
	info, err := imaging.Decode(limited)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return domain.Avatar{}, err
	}
	if int64(len(info.Data)) > s.maxFileSize {
		span.SetStatus(codes.Error, ErrFileTooLarge.Error())
		return domain.Avatar{}, ErrFileTooLarge
	}

	id := uuid.NewString()
	ext := extensionByMime(info.MimeType, filepath.Ext(in.FileName))
	key := fmt.Sprintf("avatars/%s/original%s", id, ext)
	now := time.Now().UTC()
	avatar := domain.Avatar{
		ID:               id,
		UserID:           in.UserID,
		FileName:         in.FileName,
		MimeType:         info.MimeType,
		SizeBytes:        int64(len(info.Data)),
		Dimensions:       domain.Dimensions{Width: info.Width, Height: info.Height},
		S3Key:            key,
		UploadStatus:     domain.UploadStatusCompleted,
		ProcessingStatus: domain.ProcessingStatusPending,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	span.SetAttributes(
		attribute.String("avatar_id", avatar.ID),
		attribute.String("mime_type", avatar.MimeType),
		attribute.Int("image_width", avatar.Dimensions.Width),
		attribute.Int("image_height", avatar.Dimensions.Height),
	)
	slog.LogAttrs(ctx, slog.LevelInfo, "uploading avatar", append(observability.LogAttrs(ctx),
		slog.String("user_id", avatar.UserID),
		slog.String("avatar_id", avatar.ID),
		slog.Int64("file_size", avatar.SizeBytes),
		slog.String("mime_type", avatar.MimeType),
	)...)

	if err := s.storage.Upload(ctx, key, info.MimeType, avatar.SizeBytes, bytes.NewReader(info.Data)); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return domain.Avatar{}, err
	}
	event := domain.AvatarUploadEvent{
		MessageID: uuid.NewString(),
		AvatarID:  avatar.ID,
		UserID:    avatar.UserID,
		S3Key:     avatar.S3Key,
	}

	created, err := s.repo.CreateWithUploadEvent(ctx, avatar, event)
	if err != nil {
		if deleteErr := s.storage.Delete(ctx, key); deleteErr != nil {
			slog.LogAttrs(ctx, slog.LevelError, "cleanup uploaded object after db error", append(observability.LogAttrs(ctx),
				slog.String("s3_key", key),
				slog.Any("error", deleteErr),
			)...)
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return domain.Avatar{}, err
	}
	if err := s.publisher.PublishUpload(ctx, event); err != nil {
		slog.LogAttrs(ctx, slog.LevelWarn, "publish upload event failed, event remains in outbox", append(observability.LogAttrs(ctx),
			slog.String("message_id", event.MessageID),
			slog.Any("error", err),
		)...)
		status = "accepted_outbox"
		observability.AddStorageUsage(created.UserID, created.SizeBytes)
		return created, nil
	}
	if err := s.repo.MarkOutboxPublished(ctx, event.MessageID); err != nil {
		slog.LogAttrs(ctx, slog.LevelWarn, "mark outbox message published", append(observability.LogAttrs(ctx),
			slog.String("message_id", event.MessageID),
			slog.Any("error", err),
		)...)
	}
	status = "success"
	observability.AddStorageUsage(created.UserID, created.SizeBytes)
	return created, nil
}

// Get возвращает метаданные аватара и поток с оригинальным файлом из хранилища.
func (s *AvatarService) Get(ctx context.Context, id string) (domain.Avatar, io.ReadCloser, string, error) {
	ctx, span := observability.Tracer().Start(ctx, "service.get_avatar")
	defer span.End()
	span.SetAttributes(attribute.String("avatar_id", id))
	avatar, err := s.repo.GetByID(ctx, id)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return domain.Avatar{}, nil, "", err
	}
	body, contentType, err := s.storage.Download(ctx, avatar.S3Key)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return avatar, body, contentType, err
}

// GetLatestForUser возвращает последний не удаленный аватар пользователя вместе с файлом.
func (s *AvatarService) GetLatestForUser(ctx context.Context, userID string) (domain.Avatar, io.ReadCloser, string, error) {
	ctx, span := observability.Tracer().Start(ctx, "service.get_latest_avatar")
	defer span.End()
	span.SetAttributes(attribute.String("user_id", userID))
	avatar, err := s.repo.GetLatestByUserID(ctx, userID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return domain.Avatar{}, nil, "", err
	}
	body, contentType, err := s.storage.Download(ctx, avatar.S3Key)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return avatar, body, contentType, err
}

// Metadata возвращает только данные об аватаре без скачивания файла из S3.
func (s *AvatarService) Metadata(ctx context.Context, id string) (domain.Avatar, error) {
	ctx, span := observability.Tracer().Start(ctx, "service.avatar_metadata")
	defer span.End()
	span.SetAttributes(attribute.String("avatar_id", id))
	avatar, err := s.repo.GetByID(ctx, id)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return avatar, err
}

// LatestMetadata возвращает последний аватар пользователя без скачивания файла из S3.
func (s *AvatarService) LatestMetadata(ctx context.Context, userID string) (domain.Avatar, error) {
	ctx, span := observability.Tracer().Start(ctx, "service.latest_avatar_metadata")
	defer span.End()
	span.SetAttributes(attribute.String("user_id", userID))
	avatar, err := s.repo.GetLatestByUserID(ctx, userID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return avatar, err
}

// List возвращает все не удаленные аватары пользователя, начиная с самых новых.
func (s *AvatarService) List(ctx context.Context, userID string) ([]domain.Avatar, error) {
	ctx, span := observability.Tracer().Start(ctx, "service.list_user_avatars")
	defer span.End()
	span.SetAttributes(attribute.String("user_id", userID))
	avatars, err := s.repo.ListByUserID(ctx, userID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return avatars, err
}

// PublishOutboxMessage публикует сохраненное в БД событие и помечает его отправленным.
func (s *AvatarService) PublishOutboxMessage(ctx context.Context, message domain.OutboxMessage) error {
	ctx, span := observability.Tracer().Start(ctx, "service.publish_outbox_message")
	defer span.End()
	span.SetAttributes(attribute.String("message_id", message.ID), attribute.String("routing_key", message.RoutingKey))
	switch message.RoutingKey {
	case "avatar.uploaded":
		var event domain.AvatarUploadEvent
		if err := json.Unmarshal(message.Payload, &event); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
		if err := s.publisher.PublishUpload(ctx, event); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
		if err := s.repo.MarkOutboxPublished(ctx, message.ID); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
		return nil
	case "avatar.deleted":
		var event domain.AvatarDeleteEvent
		if err := json.Unmarshal(message.Payload, &event); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
		if err := s.publisher.PublishDelete(ctx, event); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
		if err := s.repo.MarkOutboxPublished(ctx, message.ID); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
		return nil
	default:
		err := fmt.Errorf("unknown outbox routing key: %s", message.RoutingKey)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
}

// FlushOutbox публикует пачку событий, которые остались в outbox после временных ошибок RabbitMQ.
func (s *AvatarService) FlushOutbox(ctx context.Context, limit int) error {
	ctx, span := observability.Tracer().Start(ctx, "service.flush_outbox")
	defer span.End()
	span.SetAttributes(attribute.Int("limit", limit))
	messages, err := s.repo.PendingOutbox(ctx, limit)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	for _, message := range messages {
		if err := s.PublishOutboxMessage(ctx, message); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
	}
	return nil
}

// Delete делает soft delete в базе и публикует событие на удаление файлов из S3.
func (s *AvatarService) Delete(ctx context.Context, id, userID string) error {
	ctx, span := observability.Tracer().Start(ctx, "service.delete_avatar")
	defer span.End()
	span.SetAttributes(attribute.String("avatar_id", id), attribute.String("user_id", userID))
	status := "error"
	defer func() { observability.ObserveDelete(status) }()

	avatar, event, err := s.repo.SoftDeleteWithDeleteEvent(ctx, id, userID, uuid.NewString())
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if err := s.publisher.PublishDelete(ctx, event); err != nil {
		slog.LogAttrs(ctx, slog.LevelWarn, "publish delete event failed, event remains in outbox", append(observability.LogAttrs(ctx),
			slog.String("message_id", event.MessageID),
			slog.Any("error", err),
		)...)
		status = "accepted_outbox"
		observability.AddStorageUsage(avatar.UserID, -avatar.SizeBytes)
		return nil
	}
	if err := s.repo.MarkOutboxPublished(ctx, event.MessageID); err != nil {
		slog.LogAttrs(ctx, slog.LevelWarn, "mark outbox message published", append(observability.LogAttrs(ctx),
			slog.String("message_id", event.MessageID),
			slog.Any("error", err),
		)...)
	}
	status = "success"
	observability.AddStorageUsage(avatar.UserID, -avatar.SizeBytes)
	return nil
}

func extensionByMime(mimeType, fallback string) string {
	switch mimeType {
	case "image/jpeg":
		return ".jpg"
	case "image/png":
		return ".png"
	case "image/webp":
		return ".webp"
	default:
		if fallback != "" {
			return fallback
		}
		return ".img"
	}
}
