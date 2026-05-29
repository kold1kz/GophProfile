package circuitbreaker

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var ErrOpen = errors.New("circuit breaker is open")

type state int

const (
	stateClosed state = iota
	stateOpen
	stateHalfOpen
)

type Option func(*CircuitBreaker)

type CircuitBreaker struct {
	name        string
	maxFailures int
	openFor     time.Duration
	ignoreError func(error) bool

	mu       sync.Mutex
	state    state
	failures int
	openedAt time.Time
}

func New(name string, opts ...Option) *CircuitBreaker {
	b := &CircuitBreaker{name: name, maxFailures: 3, openFor: 10 * time.Second}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

func WithIgnoredErrors(fn func(error) bool) Option {
	return func(b *CircuitBreaker) {
		b.ignoreError = fn
	}
}

func Execute[T any](b *CircuitBreaker, fn func() (T, error)) (T, error) {
	var zero T
	if b == nil {
		return fn()
	}
	if err := b.beforeCall(); err != nil {
		return zero, err
	}
	result, err := fn()
	b.afterCall(err)
	return result, err
}

func ExecuteVoid(b *CircuitBreaker, fn func() error) error {
	_, err := Execute(b, func() (struct{}, error) {
		return struct{}{}, fn()
	})
	return err
}

func (b *CircuitBreaker) beforeCall() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state == stateOpen {
		if time.Since(b.openedAt) < b.openFor {
			return fmt.Errorf("%s: %w", b.name, ErrOpen)
		}
		b.state = stateHalfOpen
	}
	return nil
}

func (b *CircuitBreaker) afterCall(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err == nil || (b.ignoreError != nil && b.ignoreError(err)) {
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
