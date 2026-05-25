package delivery

import (
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type CircuitState int

const (
	StateClosed   CircuitState = iota
	StateOpen
	StateHalfOpen
)

func (s CircuitState) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

type CircuitBreaker interface {
	Allow() bool
	RecordSuccess()
	RecordFailure()
	State() CircuitState
}

type circuitBreaker struct {
	mu               sync.RWMutex
	state            CircuitState
	failures         int
	failureThreshold int
	openDuration     time.Duration
	halfOpenMax      int
	halfOpenCount    int
	openedAt         time.Time
	onStateChange    func(from, to CircuitState)
}

type CircuitBreakerConfig struct {
	FailureThreshold int
	OpenDuration     time.Duration
	HalfOpenMax      int
	OnStateChange    func(from, to CircuitState)
}

func NewCircuitBreaker(cfg CircuitBreakerConfig) CircuitBreaker {
	return &circuitBreaker{
		state:            StateClosed,
		failureThreshold: cfg.FailureThreshold,
		openDuration:     cfg.OpenDuration,
		halfOpenMax:      cfg.HalfOpenMax,
		onStateChange:    cfg.OnStateChange,
	}
}

func (cb *circuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		return true
	case StateOpen:
		if time.Since(cb.openedAt) >= cb.openDuration {
			cb.transitionTo(StateHalfOpen)
			cb.halfOpenCount = 0
			return true
		}
		return false
	case StateHalfOpen:
		if cb.halfOpenCount < cb.halfOpenMax {
			cb.halfOpenCount++
			return true
		}
		return false
	default:
		return false
	}
}

func (cb *circuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateHalfOpen:
		cb.transitionTo(StateClosed)
		cb.failures = 0
	case StateClosed:
		cb.failures = 0
	}
}

func (cb *circuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures++

	switch cb.state {
	case StateClosed:
		if cb.failures >= cb.failureThreshold {
			cb.transitionTo(StateOpen)
			cb.openedAt = time.Now()
		}
	case StateHalfOpen:
		cb.transitionTo(StateOpen)
		cb.openedAt = time.Now()
	}
}

func (cb *circuitBreaker) State() CircuitState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

func (cb *circuitBreaker) transitionTo(to CircuitState) {
	from := cb.state
	cb.state = to
	if cb.onStateChange != nil && from != to {
		cb.onStateChange(from, to)
	}
}

type CircuitBreakerRegistry struct {
	mu          sync.RWMutex
	breakers    map[string]CircuitBreaker
	cfg         CircuitBreakerConfig
	redisClient *redis.Client
}

func NewCircuitBreakerRegistry(cfg CircuitBreakerConfig) *CircuitBreakerRegistry {
	return &CircuitBreakerRegistry{
		breakers: make(map[string]CircuitBreaker),
		cfg:      cfg,
	}
}

func (r *CircuitBreakerRegistry) Get(channel string) CircuitBreaker {
	r.mu.RLock()
	cb, ok := r.breakers[channel]
	r.mu.RUnlock()
	if ok {
		return cb
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if cb, ok := r.breakers[channel]; ok {
		return cb
	}

	if r.redisClient != nil {
		cb = NewRedisCircuitBreaker(r.redisClient, channel, r.cfg)
	} else {
		cfg := r.cfg
		origOnChange := cfg.OnStateChange
		cfg.OnStateChange = func(from, to CircuitState) {
			if origOnChange != nil {
				origOnChange(from, to)
			}
			_ = fmt.Sprintf("circuit breaker %s: %s -> %s", channel, from, to)
		}
		cb = NewCircuitBreaker(cfg)
	}

	r.breakers[channel] = cb
	return cb
}
