package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/sertacyildirim/notification-system/shared/domain"
)

type mockService struct {
	notification *domain.Notification
	err          error
}

func (m *mockService) Create(ctx context.Context, req domain.CreateNotificationRequest, idempotencyKey string) (*domain.Notification, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.notification, nil
}

func (m *mockService) CreateBatch(ctx context.Context, req domain.BatchCreateRequest) (*uuid.UUID, []*domain.Notification, error) {
	if m.err != nil {
		return nil, nil, m.err
	}
	batchID := uuid.New()
	return &batchID, []*domain.Notification{m.notification}, nil
}

func (m *mockService) GetByID(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.notification, nil
}

func (m *mockService) GetByBatchID(ctx context.Context, batchID uuid.UUID) ([]*domain.Notification, error) {
	if m.err != nil {
		return nil, m.err
	}
	return []*domain.Notification{m.notification}, nil
}

func (m *mockService) Cancel(ctx context.Context, id uuid.UUID) error {
	return m.err
}

func (m *mockService) List(ctx context.Context, req domain.ListNotificationsRequest) ([]*domain.Notification, int64, error) {
	if m.err != nil {
		return nil, 0, m.err
	}
	return []*domain.Notification{m.notification}, 1, nil
}

func newTestNotification() *domain.Notification {
	return &domain.Notification{
		ID:        uuid.New(),
		Recipient: "+905551234567",
		Channel:   domain.ChannelSMS,
		Content:   "test",
		Priority:  domain.PriorityNormal,
		Status:    domain.StatusPending,
	}
}

func TestCreate_Success(t *testing.T) {
	n := newTestNotification()
	h := NewNotificationHandler(&mockService{notification: n})

	body := `{"recipient":"+905551234567","channel":"sms","content":"hello","priority":"normal"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/notifications", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	h.Create(rr, req)

	if rr.Code != http.StatusCreated {
		t.Errorf("expected 201, got %d", rr.Code)
	}

	var resp domain.APIResponse
	json.NewDecoder(rr.Body).Decode(&resp)
	if !resp.Success {
		t.Error("expected success=true")
	}
}

func TestCreate_InvalidBody(t *testing.T) {
	h := NewNotificationHandler(&mockService{})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/notifications", bytes.NewBufferString("invalid"))
	rr := httptest.NewRecorder()
	h.Create(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}
}

func TestGetByID_Success(t *testing.T) {
	n := newTestNotification()
	h := NewNotificationHandler(&mockService{notification: n})

	r := chi.NewRouter()
	r.Get("/api/v1/notifications/{id}", h.GetByID)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications/"+n.ID.String(), nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
}

func TestGetByID_NotFound(t *testing.T) {
	h := NewNotificationHandler(&mockService{notification: nil})

	r := chi.NewRouter()
	r.Get("/api/v1/notifications/{id}", h.GetByID)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications/"+uuid.New().String(), nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rr.Code)
	}
}

func TestGetByID_InvalidID(t *testing.T) {
	h := NewNotificationHandler(&mockService{})

	r := chi.NewRouter()
	r.Get("/api/v1/notifications/{id}", h.GetByID)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications/not-a-uuid", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}
}

func TestCancel_Conflict(t *testing.T) {
	h := NewNotificationHandler(&mockService{
		err: fmt.Errorf("cannot cancel notification in processing status"),
	})

	r := chi.NewRouter()
	r.Patch("/api/v1/notifications/{id}/cancel", h.Cancel)

	req := httptest.NewRequest(http.MethodPatch, "/api/v1/notifications/"+uuid.New().String()+"/cancel", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusConflict {
		t.Errorf("expected 409, got %d", rr.Code)
	}
}

func TestList_Success(t *testing.T) {
	n := newTestNotification()
	h := NewNotificationHandler(&mockService{notification: n})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications?status=pending&limit=10", nil)
	rr := httptest.NewRecorder()
	h.List(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
}
