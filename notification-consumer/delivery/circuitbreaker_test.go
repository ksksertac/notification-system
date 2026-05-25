package delivery

import (
	"testing"
	"time"
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
