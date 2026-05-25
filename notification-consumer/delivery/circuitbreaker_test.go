package delivery

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestCircuitBreaker_StartsClosedAndAllows(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 3,
		OpenDuration:     1 * time.Second,
		HalfOpenMax:      1,
	})

	if cb.State() != StateClosed {
		t.Fatalf("expected StateClosed, got %v", cb.State())
	}
	if !cb.Allow() {
		t.Fatal("expected Allow() = true when closed")
	}
}

func TestCircuitBreaker_OpensAfterThreshold(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 3,
		OpenDuration:     1 * time.Second,
		HalfOpenMax:      1,
	})

	cb.RecordFailure()
	cb.RecordFailure()
	if cb.State() != StateClosed {
		t.Fatal("should still be closed after 2 failures")
	}

	cb.RecordFailure()
	if cb.State() != StateOpen {
		t.Fatalf("expected StateOpen after 3 failures, got %v", cb.State())
	}
	if cb.Allow() {
		t.Fatal("expected Allow() = false when open")
	}
}

func TestCircuitBreaker_TransitionsToHalfOpen(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 1,
		OpenDuration:     50 * time.Millisecond,
		HalfOpenMax:      1,
	})

	cb.RecordFailure()
	if cb.State() != StateOpen {
		t.Fatal("expected StateOpen")
	}

	time.Sleep(60 * time.Millisecond)

	if !cb.Allow() {
		t.Fatal("expected Allow() = true after open duration (half-open)")
	}
	if cb.State() != StateHalfOpen {
		t.Fatalf("expected StateHalfOpen, got %v", cb.State())
	}
}

func TestCircuitBreaker_ClosesOnSuccessInHalfOpen(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 1,
		OpenDuration:     50 * time.Millisecond,
		HalfOpenMax:      1,
	})

	cb.RecordFailure()
	time.Sleep(60 * time.Millisecond)
	cb.Allow()

	cb.RecordSuccess()
	if cb.State() != StateClosed {
		t.Fatalf("expected StateClosed after success in half-open, got %v", cb.State())
	}
}

func TestCircuitBreaker_ReopensOnFailureInHalfOpen(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 1,
		OpenDuration:     50 * time.Millisecond,
		HalfOpenMax:      1,
	})

	cb.RecordFailure()
	time.Sleep(60 * time.Millisecond)
	cb.Allow()

	cb.RecordFailure()
	if cb.State() != StateOpen {
		t.Fatalf("expected StateOpen after failure in half-open, got %v", cb.State())
	}
}

func TestCircuitBreaker_SuccessResetFailureCount(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 3,
		OpenDuration:     1 * time.Second,
		HalfOpenMax:      1,
	})

	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordSuccess()
	cb.RecordFailure()
	cb.RecordFailure()

	if cb.State() != StateClosed {
		t.Fatal("success should have reset failure count, so 2 more failures shouldn't open")
	}
}

func TestCircuitBreakerRegistry(t *testing.T) {
	reg := NewCircuitBreakerRegistry(CircuitBreakerConfig{
		FailureThreshold: 3,
		OpenDuration:     1 * time.Second,
		HalfOpenMax:      1,
	})

	sms := reg.Get("sms")
	email := reg.Get("email")

	if sms == email {
		t.Fatal("different channels should get different circuit breakers")
	}

	sms2 := reg.Get("sms")
	if sms != sms2 {
		t.Fatal("same channel should return same circuit breaker")
	}
}

func TestRedisCircuitBreaker_FullLifecycle(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	cfg := CircuitBreakerConfig{
		FailureThreshold: 3,
		OpenDuration:     100 * time.Millisecond,
		HalfOpenMax:      1,
	}

	cb := NewRedisCircuitBreaker(client, "sms", cfg)

	// Starts closed and allows
	if cb.State() != StateClosed {
		t.Fatalf("expected StateClosed, got %v", cb.State())
	}
	if !cb.Allow() {
		t.Fatal("expected Allow()=true when closed")
	}

	// Record failures up to threshold
	cb.RecordFailure()
	cb.RecordFailure()
	if cb.State() != StateClosed {
		t.Fatal("should still be closed after 2 failures")
	}

	cb.RecordFailure()
	if cb.State() != StateOpen {
		t.Fatalf("expected StateOpen after 3 failures, got %v", cb.State())
	}
	if cb.Allow() {
		t.Fatal("expected Allow()=false when open")
	}

	// After open duration, transitions to half-open
	time.Sleep(120 * time.Millisecond)
	if !cb.Allow() {
		t.Fatal("expected Allow()=true after open duration (half-open)")
	}
	if cb.State() != StateHalfOpen {
		t.Fatalf("expected StateHalfOpen, got %v", cb.State())
	}

	// Success in half-open closes the breaker
	cb.RecordSuccess()
	if cb.State() != StateClosed {
		t.Fatalf("expected StateClosed after success in half-open, got %v", cb.State())
	}
}

func TestRedisCircuitBreaker_ReopensOnHalfOpenFailure(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	cfg := CircuitBreakerConfig{
		FailureThreshold: 1,
		OpenDuration:     50 * time.Millisecond,
		HalfOpenMax:      1,
	}

	cb := NewRedisCircuitBreaker(client, "email", cfg)

	cb.RecordFailure()
	if cb.State() != StateOpen {
		t.Fatal("expected StateOpen")
	}

	time.Sleep(60 * time.Millisecond)
	cb.Allow() // transitions to half-open

	cb.RecordFailure()
	if cb.State() != StateOpen {
		t.Fatalf("expected StateOpen after failure in half-open, got %v", cb.State())
	}
}

func TestCircuitState_String(t *testing.T) {
	tests := []struct {
		state    CircuitState
		expected string
	}{
		{StateClosed, "closed"},
		{StateOpen, "open"},
		{StateHalfOpen, "half-open"},
		{CircuitState(99), "unknown"},
	}

	for _, tt := range tests {
		got := tt.state.String()
		if got != tt.expected {
			t.Errorf("CircuitState(%d).String() = %s, want %s", int(tt.state), got, tt.expected)
		}
	}
}

func TestCircuitBreaker_DefaultCaseReturnsFalse(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 3,
		OpenDuration:     1 * time.Second,
		HalfOpenMax:      1,
	})

	// Force an invalid state by directly manipulating the struct
	impl := cb.(*circuitBreaker)
	impl.mu.Lock()
	impl.state = CircuitState(99) // invalid state
	impl.mu.Unlock()

	if cb.Allow() {
		t.Fatal("expected Allow() = false for unknown state (default case)")
	}
}

func TestCircuitBreaker_TransitionToSameState(t *testing.T) {
	// Test that transitioning to the same state does NOT call the callback
	callbackCalled := false
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 3,
		OpenDuration:     1 * time.Second,
		HalfOpenMax:      1,
		OnStateChange: func(from, to CircuitState) {
			callbackCalled = true
		},
	})

	// Record success while already closed - should not trigger state change callback
	cb.RecordSuccess()
	if callbackCalled {
		t.Fatal("expected callback NOT to be called when state doesn't change")
	}
}

func TestCircuitBreaker_HalfOpenMaxExceeded(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 1,
		OpenDuration:     50 * time.Millisecond,
		HalfOpenMax:      1,
	})

	cb.RecordFailure() // open
	time.Sleep(60 * time.Millisecond)

	// First call transitions from open to half-open (halfOpenCount set to 0), returns true
	if !cb.Allow() {
		t.Fatal("expected first Allow() to succeed (transitions to half-open)")
	}
	// Second call: halfOpenCount(0) < halfOpenMax(1), increments to 1, returns true
	if !cb.Allow() {
		t.Fatal("expected second Allow() in half-open to succeed")
	}
	// Third call: halfOpenCount(1) >= halfOpenMax(1), denied
	if cb.Allow() {
		t.Fatal("expected Allow() to be denied after halfOpenMax exceeded")
	}
}

func TestCircuitBreaker_OnStateChangeCallback(t *testing.T) {
	var transitions []struct{ from, to CircuitState }
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 1,
		OpenDuration:     50 * time.Millisecond,
		HalfOpenMax:      1,
		OnStateChange: func(from, to CircuitState) {
			transitions = append(transitions, struct{ from, to CircuitState }{from, to})
		},
	})

	cb.RecordFailure() // closed -> open
	if len(transitions) != 1 || transitions[0].from != StateClosed || transitions[0].to != StateOpen {
		t.Fatalf("expected closed->open transition, got %+v", transitions)
	}

	time.Sleep(60 * time.Millisecond)
	cb.Allow() // open -> half-open
	if len(transitions) != 2 || transitions[1].from != StateOpen || transitions[1].to != StateHalfOpen {
		t.Fatalf("expected open->half-open transition, got %+v", transitions)
	}

	cb.RecordSuccess() // half-open -> closed
	if len(transitions) != 3 || transitions[2].from != StateHalfOpen || transitions[2].to != StateClosed {
		t.Fatalf("expected half-open->closed transition, got %+v", transitions)
	}
}

func TestCircuitBreakerRegistry_ConcurrentAccess(t *testing.T) {
	reg := NewCircuitBreakerRegistry(CircuitBreakerConfig{
		FailureThreshold: 3,
		OpenDuration:     1 * time.Second,
		HalfOpenMax:      1,
	})

	done := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func() {
			cb := reg.Get("concurrent-channel")
			cb.Allow()
			cb.RecordSuccess()
			done <- struct{}{}
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify we get the same breaker
	cb := reg.Get("concurrent-channel")
	if cb == nil {
		t.Fatal("expected non-nil circuit breaker after concurrent access")
	}
}

func TestRedisCircuitBreaker_String(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	cfg := CircuitBreakerConfig{
		FailureThreshold: 5,
		OpenDuration:     30 * time.Second,
		HalfOpenMax:      2,
	}

	cb := NewRedisCircuitBreaker(client, "sms", cfg)
	s := cb.String()

	if s == "" {
		t.Fatal("expected non-empty string")
	}
	expected := "RedisCircuitBreaker{key=cb:sms, threshold=5, openDuration=30s}"
	if s != expected {
		t.Errorf("expected %q, got %q", expected, s)
	}
}

func TestRedisCircuitBreaker_AllowFailsOpen(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	cfg := CircuitBreakerConfig{
		FailureThreshold: 3,
		OpenDuration:     1 * time.Second,
		HalfOpenMax:      1,
	}

	cb := NewRedisCircuitBreaker(client, "test", cfg)

	// Close miniredis to force errors
	mr.Close()

	// Should fail open (return true) when Redis is down
	if !cb.Allow() {
		t.Fatal("expected Allow() to fail open (return true) when Redis is down")
	}
}

func TestRedisCircuitBreaker_StateReturnsClosedOnError(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	cfg := CircuitBreakerConfig{
		FailureThreshold: 3,
		OpenDuration:     1 * time.Second,
		HalfOpenMax:      1,
	}

	cb := NewRedisCircuitBreaker(client, "test", cfg)

	// Close miniredis to force errors
	mr.Close()

	// Should return StateClosed on error
	if cb.State() != StateClosed {
		t.Fatalf("expected StateClosed on error, got %v", cb.State())
	}
}

func TestRedisCircuitBreaker_HalfOpenMaxExceeded(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	cfg := CircuitBreakerConfig{
		FailureThreshold: 1,
		OpenDuration:     50 * time.Millisecond,
		HalfOpenMax:      2,
	}

	cb := NewRedisCircuitBreaker(client, "halfopen-test", cfg)

	// Open the breaker
	cb.RecordFailure()
	if cb.State() != StateOpen {
		t.Fatal("expected StateOpen")
	}

	// Wait for open duration
	time.Sleep(60 * time.Millisecond)

	// First Allow transitions to half-open, count = 1
	if !cb.Allow() {
		t.Fatal("expected first Allow() to succeed")
	}
	// Second Allow increments to count = 2
	if !cb.Allow() {
		t.Fatal("expected second Allow() to succeed (halfOpenMax=2)")
	}
	// Third Allow should be denied (count >= halfOpenMax)
	if cb.Allow() {
		t.Fatal("expected Allow() to be denied after halfOpenMax exceeded")
	}
}

func TestRedisCircuitBreakerRegistry_SharedState(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	cfg := CircuitBreakerConfig{
		FailureThreshold: 2,
		OpenDuration:     1 * time.Second,
		HalfOpenMax:      1,
	}

	reg := NewRedisCircuitBreakerRegistry(client, cfg)

	cb1 := reg.Get("push")
	cb2 := reg.Get("push")

	if cb1 != cb2 {
		t.Fatal("same channel should return same circuit breaker instance")
	}

	// Different channels get different breakers
	cbSMS := reg.Get("sms")
	if cb1 == cbSMS {
		t.Fatal("different channels should return different breakers")
	}
}
