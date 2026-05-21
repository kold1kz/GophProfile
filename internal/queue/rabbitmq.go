package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"gophprofile/internal/domain"
	"gophprofile/internal/observability"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

const (
	// RoutingAvatarUploaded используется для событий о новой загрузке аватара.
	RoutingAvatarUploaded = "avatar.uploaded"

	// RoutingAvatarDeleted используется для событий об удалении аватара и связанных файлов.
	RoutingAvatarDeleted = "avatar.deleted"

	confirmTimeout = 5 * time.Second
)

// RabbitMQ инкапсулирует подключение к брокеру, exchange, рабочую очередь и DLQ.
type RabbitMQ struct {
	mu         sync.RWMutex
	publishMu  sync.Mutex
	url        string
	conn       *amqp.Connection
	publishCh  *amqp.Channel
	consumeCh  *amqp.Channel
	confirms   <-chan amqp.Confirmation
	returns    <-chan amqp.Return
	closeCh    <-chan *amqp.Error
	exchange   string
	queue      string
	dlx        string
	deadLetter string
}

// NewRabbitMQ подключается к RabbitMQ и объявляет все сущности, нужные для надежной доставки.
func NewRabbitMQ(url, exchange, queue string) (*RabbitMQ, error) {
	r := &RabbitMQ{
		url:        url,
		exchange:   exchange,
		queue:      queue,
		dlx:        exchange + ".dlx",
		deadLetter: queue + ".dlq",
	}
	if err := r.connect(); err != nil {
		return nil, err
	}
	go r.reconnectOnClose()
	return r, nil
}

func (r *RabbitMQ) connect() error {
	conn, err := amqp.Dial(r.url)
	if err != nil {
		return err
	}
	publishCh, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return err
	}
	consumeCh, err := conn.Channel()
	if err != nil {
		_ = publishCh.Close()
		_ = conn.Close()
		return err
	}

	r.mu.Lock()
	oldConn := r.conn
	oldPublishCh := r.publishCh
	oldConsumeCh := r.consumeCh
	r.conn = conn
	r.publishCh = publishCh
	r.consumeCh = consumeCh
	r.closeCh = conn.NotifyClose(make(chan *amqp.Error, 1))
	r.mu.Unlock()

	if oldPublishCh != nil {
		_ = oldPublishCh.Close()
	}
	if oldConsumeCh != nil {
		_ = oldConsumeCh.Close()
	}
	if oldConn != nil {
		_ = oldConn.Close()
	}

	if err := r.declare(); err != nil {
		return err
	}
	if err := publishCh.Confirm(false); err != nil {
		return err
	}

	r.mu.Lock()
	r.confirms = publishCh.NotifyPublish(make(chan amqp.Confirmation, 1))
	r.returns = publishCh.NotifyReturn(make(chan amqp.Return, 1))
	r.mu.Unlock()
	return nil
}

func (r *RabbitMQ) declare() error {
	r.mu.RLock()
	ch := r.consumeCh
	r.mu.RUnlock()

	if err := ch.ExchangeDeclare(r.exchange, "topic", true, false, false, false, nil); err != nil {
		return err
	}
	if err := ch.ExchangeDeclare(r.dlx, "topic", true, false, false, false, nil); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(r.deadLetter, true, false, false, false, nil); err != nil {
		return err
	}
	if err := ch.QueueBind(r.deadLetter, "#", r.dlx, false, nil); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(r.queue, true, false, false, false, amqp.Table{
		"x-dead-letter-exchange": r.dlx,
	}); err != nil {
		return err
	}
	for _, key := range []string{RoutingAvatarUploaded, RoutingAvatarDeleted} {
		if err := ch.QueueBind(r.queue, key, r.exchange, false, nil); err != nil {
			return err
		}
	}
	return ch.Qos(1, 0, false)
}

// PublishUpload публикует событие о новой загрузке и ждет confirm от брокера.
func (r *RabbitMQ) PublishUpload(ctx context.Context, event domain.AvatarUploadEvent) error {
	return r.publish(ctx, RoutingAvatarUploaded, event.MessageID, event)
}

// PublishDelete публикует событие удаления и ждет confirm от брокера.
func (r *RabbitMQ) PublishDelete(ctx context.Context, event domain.AvatarDeleteEvent) error {
	return r.publish(ctx, RoutingAvatarDeleted, event.MessageID, event)
}

// Consume подписывает worker на очередь в режиме manual ack.
func (r *RabbitMQ) Consume() (<-chan amqp.Delivery, error) {
	r.mu.RLock()
	ch := r.consumeCh
	r.mu.RUnlock()
	return ch.Consume(r.queue, "", false, false, false, false, nil)
}

// Ping проверяет, что exchange существует и канал RabbitMQ жив.
func (r *RabbitMQ) Ping(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	r.mu.RLock()
	ch := r.consumeCh
	r.mu.RUnlock()
	return ch.ExchangeDeclarePassive(r.exchange, "topic", true, false, false, false, nil)
}

// QueueDepth returns the current number of ready messages in the worker queue.
func (r *RabbitMQ) QueueDepth() (int, error) {
	r.mu.RLock()
	ch := r.consumeCh
	r.mu.RUnlock()
	queue, err := ch.QueueInspect(r.queue)
	if err != nil {
		return 0, err
	}
	return queue.Messages, nil
}

// Close закрывает каналы и соединение с RabbitMQ.
func (r *RabbitMQ) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.publishCh != nil {
		_ = r.publishCh.Close()
	}
	if r.consumeCh != nil {
		_ = r.consumeCh.Close()
	}
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}

func (r *RabbitMQ) publish(ctx context.Context, routingKey, messageID string, payload any) error {
	ctx, span := observability.Tracer().Start(ctx, "rabbitmq.publish")
	defer span.End()
	span.SetAttributes(attribute.String("routing_key", routingKey), attribute.String("message_id", messageID))
	r.publishMu.Lock()
	defer r.publishMu.Unlock()

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, confirmTimeout)
	defer cancel()

	r.mu.RLock()
	ch := r.publishCh
	confirms := r.confirms
	returns := r.returns
	r.mu.RUnlock()

	headers := amqp.Table{}
	otel.GetTextMapPropagator().Inject(ctx, amqpHeadersCarrier(headers))

	if err := ch.PublishWithContext(ctx, r.exchange, routingKey, true, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    messageID,
		Timestamp:    time.Now().UTC(),
		Headers:      headers,
		Body:         body,
	}); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	select {
	case ret := <-returns:
		span.SetStatus(codes.Error, ret.ReplyText)
		return fmt.Errorf("rabbitmq returned message %q: %s", ret.RoutingKey, ret.ReplyText)
	case confirmation := <-confirms:
		if confirmation.Ack {
			return nil
		}
		span.SetStatus(codes.Error, "rabbitmq rejected publish")
		return errors.New("rabbitmq rejected publish")
	case <-ctx.Done():
		span.RecordError(ctx.Err())
		span.SetStatus(codes.Error, ctx.Err().Error())
		return fmt.Errorf("rabbitmq publish confirm timeout: %w", ctx.Err())
	}
}

type amqpHeadersCarrier amqp.Table

func (c amqpHeadersCarrier) Get(key string) string {
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

func (c amqpHeadersCarrier) Set(key, value string) {
	c[key] = value
}

func (c amqpHeadersCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for key := range c {
		keys = append(keys, key)
	}
	return keys
}

func (r *RabbitMQ) reconnectOnClose() {
	for {
		r.mu.RLock()
		closeCh := r.closeCh
		r.mu.RUnlock()

		err, ok := <-closeCh
		if !ok {
			return
		}
		if err != nil {
			slog.Warn("rabbitmq connection closed", "error", err)
		}

		delay := time.Second
		for {
			if err := r.connect(); err == nil {
				slog.Info("rabbitmq connection restored")
				break
			}
			time.Sleep(delay)
			delay *= 2
			if delay > 30*time.Second {
				delay = 30 * time.Second
			}
		}
	}
}
