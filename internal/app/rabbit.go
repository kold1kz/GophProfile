package app

import (
	"context"
	"log/slog"
	"time"

	"gophprofile/internal/queue"
)

var newRabbitMQ = queue.NewRabbitMQ

// ConnectRabbit подключается к RabbitMQ с экспоненциальной задержкой между попытками.
func ConnectRabbit(ctx context.Context, url, exchange, queueName string) (*queue.RabbitMQ, error) {
	var lastErr error
	delay := time.Second
	for attempt := 1; attempt <= 20; attempt++ {
		broker, err := newRabbitMQ(url, exchange, queueName)
		if err == nil {
			return broker, nil
		}
		lastErr = err
		slog.Warn("connect rabbitmq failed", "attempt", attempt, "error", err)

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}

		delay *= 2
		if delay > 30*time.Second {
			delay = 30 * time.Second
		}
	}
	return nil, lastErr
}
