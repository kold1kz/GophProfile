package resilience

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"gophprofile/internal/domain"
	"gophprofile/internal/services"
)

var ErrCircuitOpen = errors.New("circuit breaker is open")

type circuitState int

const (
	stateClosed circuitState = iota
	stateOpen
	stateHalfOpen
)

type CircuitBreaker struct {
	name        string
	maxFailures int
	openFor     time.Duration

	mu       sync.Mutex
	state    circuitState
	failures int
	openedAt time.Time
}

func NewCircuitBreaker(name string) *CircuitBreaker {
	return &CircuitBreaker{name: name, maxFailures: 3, openFor: 10 * time.Second}
}

func (b *CircuitBreaker) Execute(fn func() error) error {
	if err := b.beforeCall(); err != nil {
		return err
	}
	err := fn()
	b.afterCall(err)
	return err
}

func (b *CircuitBreaker) beforeCall() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state == stateOpen {
		if time.Since(b.openedAt) < b.openFor {
			return fmt.Errorf("%s: %w", b.name, ErrCircuitOpen)
		}
		b.state = stateHalfOpen
	}
	return nil
}

func (b *CircuitBreaker) afterCall(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err == nil || isDomainError(err) {
		b.failures = 0
		b.state = stateClosed
		return
	}
	b.failures++
	if b.failures >= b.maxFailures || b.state == stateHalfOpen {
		b.state = stateOpen
		b.openedAt = time.Now()
	}
}

func isDomainError(err error) bool {
	return errors.Is(err, domain.ErrNotFound) || errors.Is(err, domain.ErrForbidden)
}

type Repository struct {
	next    services.AvatarRepository
	breaker *CircuitBreaker
}

func NewRepository(next services.AvatarRepository) *Repository {
	return &Repository{next: next, breaker: NewCircuitBreaker("postgres")}
}

func (r *Repository) CreateWithUploadEvent(ctx context.Context, avatar domain.Avatar, event domain.AvatarUploadEvent) (domain.Avatar, error) {
	var result domain.Avatar
	err := r.breaker.Execute(func() error {
		var err error
		result, err = r.next.CreateWithUploadEvent(ctx, avatar, event)
		return err
	})
	return result, err
}

func (r *Repository) GetByID(ctx context.Context, id string) (domain.Avatar, error) {
	var result domain.Avatar
	err := r.breaker.Execute(func() error {
		var err error
		result, err = r.next.GetByID(ctx, id)
		return err
	})
	return result, err
}

func (r *Repository) GetLatestByUserID(ctx context.Context, userID string) (domain.Avatar, error) {
	var result domain.Avatar
	err := r.breaker.Execute(func() error {
		var err error
		result, err = r.next.GetLatestByUserID(ctx, userID)
		return err
	})
	return result, err
}

func (r *Repository) ListByUserID(ctx context.Context, userID string) ([]domain.Avatar, error) {
	var result []domain.Avatar
	err := r.breaker.Execute(func() error {
		var err error
		result, err = r.next.ListByUserID(ctx, userID)
		return err
	})
	return result, err
}

func (r *Repository) SoftDelete(ctx context.Context, id, userID string) (domain.Avatar, error) {
	var result domain.Avatar
	err := r.breaker.Execute(func() error {
		var err error
		result, err = r.next.SoftDelete(ctx, id, userID)
		return err
	})
	return result, err
}

func (r *Repository) SoftDeleteWithDeleteEvent(ctx context.Context, id, userID, messageID string) (domain.Avatar, domain.AvatarDeleteEvent, error) {
	var avatar domain.Avatar
	var event domain.AvatarDeleteEvent
	err := r.breaker.Execute(func() error {
		var err error
		avatar, event, err = r.next.SoftDeleteWithDeleteEvent(ctx, id, userID, messageID)
		return err
	})
	return avatar, event, err
}

func (r *Repository) MarkProcessing(ctx context.Context, id string) (bool, error) {
	var result bool
	err := r.breaker.Execute(func() error {
		var err error
		result, err = r.next.MarkProcessing(ctx, id)
		return err
	})
	return result, err
}

func (r *Repository) UpdateProcessed(ctx context.Context, id string, thumbnails []domain.Thumbnail) error {
	return r.breaker.Execute(func() error { return r.next.UpdateProcessed(ctx, id, thumbnails) })
}

func (r *Repository) UpdateProcessingFailed(ctx context.Context, id string) error {
	return r.breaker.Execute(func() error { return r.next.UpdateProcessingFailed(ctx, id) })
}

func (r *Repository) ProcessedMessage(ctx context.Context, messageID string) (bool, error) {
	var result bool
	err := r.breaker.Execute(func() error {
		var err error
		result, err = r.next.ProcessedMessage(ctx, messageID)
		return err
	})
	return result, err
}

func (r *Repository) SaveProcessedMessage(ctx context.Context, messageID string) error {
	return r.breaker.Execute(func() error { return r.next.SaveProcessedMessage(ctx, messageID) })
}

func (r *Repository) MarkOutboxPublished(ctx context.Context, messageID string) error {
	return r.breaker.Execute(func() error { return r.next.MarkOutboxPublished(ctx, messageID) })
}

func (r *Repository) PendingOutbox(ctx context.Context, limit int) ([]domain.OutboxMessage, error) {
	var result []domain.OutboxMessage
	err := r.breaker.Execute(func() error {
		var err error
		result, err = r.next.PendingOutbox(ctx, limit)
		return err
	})
	return result, err
}

func (r *Repository) Ping(ctx context.Context) error {
	return r.breaker.Execute(func() error { return r.next.Ping(ctx) })
}

type ObjectStorage struct {
	next    services.ObjectStorage
	breaker *CircuitBreaker
}

func NewObjectStorage(next services.ObjectStorage) *ObjectStorage {
	return &ObjectStorage{next: next, breaker: NewCircuitBreaker("s3")}
}

func (s *ObjectStorage) Upload(ctx context.Context, key, contentType string, size int64, body io.Reader) error {
	return s.breaker.Execute(func() error { return s.next.Upload(ctx, key, contentType, size, body) })
}

func (s *ObjectStorage) Download(ctx context.Context, key string) (io.ReadCloser, string, error) {
	var body io.ReadCloser
	var contentType string
	err := s.breaker.Execute(func() error {
		var err error
		body, contentType, err = s.next.Download(ctx, key)
		return err
	})
	return body, contentType, err
}

func (s *ObjectStorage) Delete(ctx context.Context, key string) error {
	return s.breaker.Execute(func() error { return s.next.Delete(ctx, key) })
}

func (s *ObjectStorage) PresignedGetURL(ctx context.Context, key string) (string, error) {
	var result string
	err := s.breaker.Execute(func() error {
		var err error
		result, err = s.next.PresignedGetURL(ctx, key)
		return err
	})
	return result, err
}

func (s *ObjectStorage) Ping(ctx context.Context) error {
	return s.breaker.Execute(func() error { return s.next.Ping(ctx) })
}

type EventPublisher struct {
	next    services.EventPublisher
	breaker *CircuitBreaker
}

func NewEventPublisher(next services.EventPublisher) *EventPublisher {
	return &EventPublisher{next: next, breaker: NewCircuitBreaker("rabbitmq")}
}

func (p *EventPublisher) PublishUpload(ctx context.Context, event domain.AvatarUploadEvent) error {
	return p.breaker.Execute(func() error { return p.next.PublishUpload(ctx, event) })
}

func (p *EventPublisher) PublishDelete(ctx context.Context, event domain.AvatarDeleteEvent) error {
	return p.breaker.Execute(func() error { return p.next.PublishDelete(ctx, event) })
}

func (p *EventPublisher) Ping(ctx context.Context) error {
	return p.breaker.Execute(func() error { return p.next.Ping(ctx) })
}
