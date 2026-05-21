package repository

import (
	"context"
	"encoding/json"
	"errors"

	"gophprofile/internal/domain"
	"gophprofile/internal/observability"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// PostgresRepository хранит и читает метаданные аватаров в PostgreSQL.
type PostgresRepository struct {
	pool *pgxpool.Pool
}

// NewPostgres оборачивает готовый pgx pool в репозиторий приложения.
func NewPostgres(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

// Connect создает пул подключений к PostgreSQL и сразу проверяет, что база отвечает.
func Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

// CreateWithUploadEvent сохраняет новый аватар и outbox-событие в одной транзакции.
func (r *PostgresRepository) CreateWithUploadEvent(ctx context.Context, avatar domain.Avatar, event domain.AvatarUploadEvent) (domain.Avatar, error) {
	ctx, span := observability.Tracer().Start(ctx, "db.create_avatar_with_upload_event")
	defer span.End()
	span.SetAttributes(attribute.String("avatar_id", avatar.ID), attribute.String("user_id", avatar.UserID))
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return domain.Avatar{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	thumbs, err := json.Marshal(avatar.ThumbnailS3Keys)
	if err != nil {
		recordSpanError(span, err)
		return domain.Avatar{}, err
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO avatars (
			id, user_id, file_name, mime_type, size_bytes, width, height, s3_key,
			thumbnail_s3_keys, upload_status, processing_status, created_at, updated_at
		)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		RETURNING created_at, updated_at`,
		avatar.ID, avatar.UserID, avatar.FileName, avatar.MimeType, avatar.SizeBytes,
		avatar.Dimensions.Width, avatar.Dimensions.Height, avatar.S3Key, thumbs,
		avatar.UploadStatus, avatar.ProcessingStatus, avatar.CreatedAt, avatar.UpdatedAt,
	).Scan(&avatar.CreatedAt, &avatar.UpdatedAt)
	if err != nil {
		recordSpanError(span, err)
		return domain.Avatar{}, err
	}

	payload, err := json.Marshal(event)
	if err != nil {
		recordSpanError(span, err)
		return domain.Avatar{}, err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_messages(message_id, routing_key, payload)
		VALUES ($1, $2, $3)
		ON CONFLICT (message_id) DO NOTHING`,
		event.MessageID, "avatar.uploaded", payload,
	)
	if err != nil {
		recordSpanError(span, err)
		return domain.Avatar{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		recordSpanError(span, err)
		return domain.Avatar{}, err
	}
	return avatar, nil
}

// GetByID ищет не удаленный аватар по идентификатору.
func (r *PostgresRepository) GetByID(ctx context.Context, id string) (domain.Avatar, error) {
	ctx, span := observability.Tracer().Start(ctx, "db.get_avatar_by_id")
	defer span.End()
	span.SetAttributes(attribute.String("avatar_id", id))
	avatar, err := r.scanOne(ctx, `
		SELECT id, user_id, file_name, mime_type, size_bytes, width, height, s3_key,
			thumbnail_s3_keys, upload_status, processing_status, created_at, updated_at, deleted_at
		FROM avatars
		WHERE id=$1 AND deleted_at IS NULL`, id)
	if err != nil {
		recordSpanError(span, err)
	}
	return avatar, err
}

// GetLatestByUserID возвращает последний не удаленный аватар конкретного пользователя.
func (r *PostgresRepository) GetLatestByUserID(ctx context.Context, userID string) (domain.Avatar, error) {
	ctx, span := observability.Tracer().Start(ctx, "db.get_latest_avatar_by_user_id")
	defer span.End()
	span.SetAttributes(attribute.String("user_id", userID))
	avatar, err := r.scanOne(ctx, `
		SELECT id, user_id, file_name, mime_type, size_bytes, width, height, s3_key,
			thumbnail_s3_keys, upload_status, processing_status, created_at, updated_at, deleted_at
		FROM avatars
		WHERE user_id=$1 AND deleted_at IS NULL
		ORDER BY created_at DESC
		LIMIT 1`, userID)
	if err != nil {
		recordSpanError(span, err)
	}
	return avatar, err
}

// ListByUserID возвращает историю не удаленных аватаров пользователя.
func (r *PostgresRepository) ListByUserID(ctx context.Context, userID string) ([]domain.Avatar, error) {
	ctx, span := observability.Tracer().Start(ctx, "db.list_avatars_by_user_id")
	defer span.End()
	span.SetAttributes(attribute.String("user_id", userID))
	rows, err := r.pool.Query(ctx, `
		SELECT id, user_id, file_name, mime_type, size_bytes, width, height, s3_key,
			thumbnail_s3_keys, upload_status, processing_status, created_at, updated_at, deleted_at
		FROM avatars
		WHERE user_id=$1 AND deleted_at IS NULL
		ORDER BY created_at DESC`, userID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	defer rows.Close()

	var avatars []domain.Avatar
	for rows.Next() {
		avatar, err := scanAvatar(rows)
		if err != nil {
			recordSpanError(span, err)
			return nil, err
		}
		avatars = append(avatars, avatar)
	}
	if err := rows.Err(); err != nil {
		recordSpanError(span, err)
		return nil, err
	}
	return avatars, nil
}

// SoftDelete помечает аватар удаленным, но оставляет запись в базе для истории и аудита.
func (r *PostgresRepository) SoftDelete(ctx context.Context, id, userID string) (domain.Avatar, error) {
	return r.softDelete(ctx, id, userID, "")
}

// SoftDeleteWithDeleteEvent помечает аватар удаленным и сохраняет событие удаления в outbox в той же транзакции.
func (r *PostgresRepository) SoftDeleteWithDeleteEvent(ctx context.Context, id, userID, messageID string) (domain.Avatar, domain.AvatarDeleteEvent, error) {
	avatar, err := r.softDelete(ctx, id, userID, messageID)
	if err != nil {
		return domain.Avatar{}, domain.AvatarDeleteEvent{}, err
	}
	return avatar, domain.AvatarDeleteEvent{MessageID: messageID, AvatarID: avatar.ID, S3Keys: s3Keys(avatar)}, nil
}

func (r *PostgresRepository) softDelete(ctx context.Context, id, userID, deleteMessageID string) (domain.Avatar, error) {
	ctx, span := observability.Tracer().Start(ctx, "db.soft_delete_avatar")
	defer span.End()
	span.SetAttributes(attribute.String("avatar_id", id), attribute.String("user_id", userID))
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return domain.Avatar{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	avatar, err := scanAvatar(tx.QueryRow(ctx, `
		SELECT id, user_id, file_name, mime_type, size_bytes, width, height, s3_key,
			thumbnail_s3_keys, upload_status, processing_status, created_at, updated_at, deleted_at
		FROM avatars
		WHERE id=$1 AND deleted_at IS NULL
		FOR UPDATE`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		recordSpanError(span, domain.ErrNotFound)
		return domain.Avatar{}, domain.ErrNotFound
	}
	if err != nil {
		recordSpanError(span, err)
		return domain.Avatar{}, err
	}
	if avatar.UserID != userID {
		recordSpanError(span, domain.ErrForbidden)
		return domain.Avatar{}, domain.ErrForbidden
	}
	if _, err = tx.Exec(ctx, `UPDATE avatars SET deleted_at=NOW(), updated_at=NOW() WHERE id=$1 AND deleted_at IS NULL`, id); err != nil {
		recordSpanError(span, err)
		return domain.Avatar{}, err
	}
	if deleteMessageID != "" {
		event := domain.AvatarDeleteEvent{
			MessageID: deleteMessageID,
			AvatarID:  avatar.ID,
			S3Keys:    s3Keys(avatar),
		}
		payload, err := json.Marshal(event)
		if err != nil {
			recordSpanError(span, err)
			return domain.Avatar{}, err
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO outbox_messages(message_id, routing_key, payload)
			VALUES ($1, $2, $3)
			ON CONFLICT (message_id) DO NOTHING`,
			event.MessageID, "avatar.deleted", payload,
		)
		if err != nil {
			recordSpanError(span, err)
			return domain.Avatar{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		recordSpanError(span, err)
		return domain.Avatar{}, err
	}
	return avatar, nil
}

func s3Keys(avatar domain.Avatar) []string {
	keys := []string{avatar.S3Key}
	for _, thumb := range avatar.ThumbnailS3Keys {
		if thumb.S3Key != "" {
			keys = append(keys, thumb.S3Key)
		}
	}
	return keys
}

// MarkProcessing пытается взять аватар в обработку и защищает worker от двойной работы.
func (r *PostgresRepository) MarkProcessing(ctx context.Context, id string) (bool, error) {
	ctx, span := observability.Tracer().Start(ctx, "db.mark_avatar_processing")
	defer span.End()
	span.SetAttributes(attribute.String("avatar_id", id))
	tag, err := r.pool.Exec(ctx, `
		UPDATE avatars
		SET processing_status=$2, updated_at=NOW()
		WHERE id=$1 AND deleted_at IS NULL AND processing_status IN ($3, $4)`,
		id, domain.ProcessingStatusProcessing, domain.ProcessingStatusPending, domain.ProcessingStatusFailed,
	)
	if err != nil {
		recordSpanError(span, err)
		return false, err
	}
	return tag.RowsAffected() == 1, err
}

// UpdateProcessed сохраняет готовые миниатюры и переводит аватар в статус completed.
func (r *PostgresRepository) UpdateProcessed(ctx context.Context, id string, thumbnails []domain.Thumbnail) error {
	ctx, span := observability.Tracer().Start(ctx, "db.update_avatar_processed")
	defer span.End()
	span.SetAttributes(attribute.String("avatar_id", id), attribute.Int("thumbnails", len(thumbnails)))
	payload, err := json.Marshal(thumbnails)
	if err != nil {
		recordSpanError(span, err)
		return err
	}
	_, err = r.pool.Exec(ctx, `
		UPDATE avatars
		SET thumbnail_s3_keys=$2, processing_status=$3, updated_at=NOW()
		WHERE id=$1 AND deleted_at IS NULL`,
		id, payload, domain.ProcessingStatusCompleted,
	)
	if err != nil {
		recordSpanError(span, err)
	}
	return err
}

// UpdateProcessingFailed помечает аватар как failed, если worker не смог его обработать.
func (r *PostgresRepository) UpdateProcessingFailed(ctx context.Context, id string) error {
	ctx, span := observability.Tracer().Start(ctx, "db.update_avatar_processing_failed")
	defer span.End()
	span.SetAttributes(attribute.String("avatar_id", id))
	_, err := r.pool.Exec(ctx, `UPDATE avatars SET processing_status=$2, updated_at=NOW() WHERE id=$1`, id, domain.ProcessingStatusFailed)
	if err != nil {
		recordSpanError(span, err)
	}
	return err
}

// ProcessedMessage проверяет, обрабатывали ли мы уже сообщение с таким MessageID.
func (r *PostgresRepository) ProcessedMessage(ctx context.Context, messageID string) (bool, error) {
	ctx, span := observability.Tracer().Start(ctx, "db.processed_message")
	defer span.End()
	span.SetAttributes(attribute.String("message_id", messageID))
	var exists bool
	err := r.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM processed_messages WHERE message_id=$1)`, messageID).Scan(&exists)
	if err != nil {
		recordSpanError(span, err)
	}
	return exists, err
}

// SaveProcessedMessage запоминает MessageID после успешной обработки события.
func (r *PostgresRepository) SaveProcessedMessage(ctx context.Context, messageID string) error {
	ctx, span := observability.Tracer().Start(ctx, "db.save_processed_message")
	defer span.End()
	span.SetAttributes(attribute.String("message_id", messageID))
	_, err := r.pool.Exec(ctx, `INSERT INTO processed_messages(message_id) VALUES ($1) ON CONFLICT DO NOTHING`, messageID)
	if err != nil {
		recordSpanError(span, err)
	}
	return err
}

// MarkOutboxPublished помечает outbox-сообщение опубликованным.
func (r *PostgresRepository) MarkOutboxPublished(ctx context.Context, messageID string) error {
	ctx, span := observability.Tracer().Start(ctx, "db.mark_outbox_published")
	defer span.End()
	span.SetAttributes(attribute.String("message_id", messageID))
	_, err := r.pool.Exec(ctx, `UPDATE outbox_messages SET published_at=NOW() WHERE message_id=$1`, messageID)
	if err != nil {
		recordSpanError(span, err)
	}
	return err
}

// PendingOutbox возвращает неопубликованные outbox-сообщения от старых к новым.
func (r *PostgresRepository) PendingOutbox(ctx context.Context, limit int) ([]domain.OutboxMessage, error) {
	ctx, span := observability.Tracer().Start(ctx, "db.pending_outbox")
	defer span.End()
	span.SetAttributes(attribute.Int("limit", limit))
	rows, err := r.pool.Query(ctx, `
		SELECT message_id, routing_key, payload, created_at
		FROM outbox_messages
		WHERE published_at IS NULL
		ORDER BY created_at
		LIMIT $1`, limit)
	if err != nil {
		recordSpanError(span, err)
		return nil, err
	}
	defer rows.Close()

	var messages []domain.OutboxMessage
	for rows.Next() {
		var message domain.OutboxMessage
		if err := rows.Scan(&message.ID, &message.RoutingKey, &message.Payload, &message.CreatedAt); err != nil {
			recordSpanError(span, err)
			return nil, err
		}
		messages = append(messages, message)
	}
	if err := rows.Err(); err != nil {
		recordSpanError(span, err)
		return nil, err
	}
	return messages, nil
}

func (r *PostgresRepository) StorageUsageByUser(ctx context.Context) (map[string]int64, error) {
	ctx, span := observability.Tracer().Start(ctx, "db.storage_usage_by_user")
	defer span.End()
	rows, err := r.pool.Query(ctx, `
		SELECT user_id, COALESCE(SUM(size_bytes), 0)
		FROM avatars
		WHERE deleted_at IS NULL
		GROUP BY user_id`)
	if err != nil {
		recordSpanError(span, err)
		return nil, err
	}
	defer rows.Close()

	usage := make(map[string]int64)
	for rows.Next() {
		var userID string
		var bytes int64
		if err := rows.Scan(&userID, &bytes); err != nil {
			recordSpanError(span, err)
			return nil, err
		}
		usage[userID] = bytes
	}
	if err := rows.Err(); err != nil {
		recordSpanError(span, err)
		return nil, err
	}
	return usage, nil
}

// Ping проверяет, что PostgreSQL доступен.
func (r *PostgresRepository) Ping(ctx context.Context) error {
	return r.pool.Ping(ctx)
}

// OpenConnections returns current PostgreSQL pool open connections for Prometheus.
func (r *PostgresRepository) OpenConnections() int32 {
	return r.pool.Stat().TotalConns()
}

func (r *PostgresRepository) scanOne(ctx context.Context, query string, args ...any) (domain.Avatar, error) {
	avatar, err := scanAvatar(r.pool.QueryRow(ctx, query, args...))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Avatar{}, domain.ErrNotFound
	}
	return avatar, err
}

type scanner interface {
	Scan(dest ...any) error
}

func scanAvatar(row scanner) (domain.Avatar, error) {
	var avatar domain.Avatar
	var thumbs []byte
	err := row.Scan(
		&avatar.ID, &avatar.UserID, &avatar.FileName, &avatar.MimeType, &avatar.SizeBytes,
		&avatar.Dimensions.Width, &avatar.Dimensions.Height, &avatar.S3Key, &thumbs,
		&avatar.UploadStatus, &avatar.ProcessingStatus, &avatar.CreatedAt, &avatar.UpdatedAt, &avatar.DeletedAt,
	)
	if err != nil {
		return domain.Avatar{}, err
	}
	if len(thumbs) > 0 {
		if err := json.Unmarshal(thumbs, &avatar.ThumbnailS3Keys); err != nil {
			return domain.Avatar{}, err
		}
	}
	return avatar, nil
}

func recordSpanError(span trace.Span, err error) {
	if err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}
