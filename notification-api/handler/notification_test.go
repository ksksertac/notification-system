package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/sertacyildirim/notification-system/shared/domain"
)

// mockService uses function fields so each test can customize behavior.
type mockService struct {
	createFn      func(ctx context.Context, req domain.CreateNotificationRequest, idempotencyKey string) (*domain.Notification, error)
	createBatchFn func(ctx context.Context, req domain.BatchCreateRequest) (*uuid.UUID, []*domain.Notification, error)
	getByIDFn     func(ctx context.Context, id uuid.UUID) (*domain.Notification, error)
	getByBatchFn  func(ctx context.Context, batchID uuid.UUID) ([]*domain.Notification, error)
	cancelFn      func(ctx context.Context, id uuid.UUID) error
	listFn        func(ctx context.Context, req domain.ListNotificationsRequest) ([]*domain.Notification, int64, error)

	// simple fields for backwards compat
	notification *domain.Notification
	err          error
}

func (m *mockService) Create(ctx context.Context, req domain.CreateNotificationRequest, idempotencyKey string) (*domain.Notification, error) {
	if m.createFn != nil {
		return m.createFn(ctx, req, idempotencyKey)
	}
	if m.err != nil {
		return nil, m.err
	}
	return m.notification, nil
}

func (m *mockService) CreateBatch(ctx context.Context, req domain.BatchCreateRequest) (*uuid.UUID, []*domain.Notification, error) {
	if m.createBatchFn != nil {
		return m.createBatchFn(ctx, req)
	}
	if m.err != nil {
		return nil, nil, m.err
	}
	batchID := uuid.New()
	return &batchID, []*domain.Notification{m.notification}, nil
}

func (m *mockService) GetByID(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	if m.getByIDFn != nil {
		return m.getByIDFn(ctx, id)
	}
	if m.err != nil {
		return nil, m.err
	}
	return m.notification, nil
}

func (m *mockService) GetByBatchID(ctx context.Context, batchID uuid.UUID) ([]*domain.Notification, error) {
	if m.getByBatchFn != nil {
		return m.getByBatchFn(ctx, batchID)
	}
	if m.err != nil {
		return nil, m.err
	}
	return []*domain.Notification{m.notification}, nil
}

func (m *mockService) Cancel(ctx context.Context, id uuid.UUID) error {
	if m.cancelFn != nil {
		return m.cancelFn(ctx, id)
	}
	return m.err
}

func (m *mockService) List(ctx context.Context, req domain.ListNotificationsRequest) ([]*domain.Notification, int64, error) {
	if m.listFn != nil {
		return m.listFn(ctx, req)
	}
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
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
}

// --- Create tests ---

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

func TestCreate_ValidationError(t *testing.T) {
	h := NewNotificationHandler(&mockService{
		err: fmt.Errorf("validation: recipient is required"),
	})

	body := `{"recipient":"","channel":"sms","content":"hello","priority":"normal"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/notifications", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.Create(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}
}

func TestCreate_InternalError(t *testing.T) {
	h := NewNotificationHandler(&mockService{
		err: fmt.Errorf("redis connection failed"),
	})

	body := `{"recipient":"+905551234567","channel":"sms","content":"hello","priority":"normal"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/notifications", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.Create(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rr.Code)
	}
}

func TestCreate_WithIdempotencyKey(t *testing.T) {
	var receivedKey string
	n := newTestNotification()
	h := NewNotificationHandler(&mockService{
		createFn: func(ctx context.Context, req domain.CreateNotificationRequest, idempotencyKey string) (*domain.Notification, error) {
			receivedKey = idempotencyKey
			return n, nil
		},
	})

	body := `{"recipient":"+905551234567","channel":"sms","content":"hello","priority":"normal"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/notifications", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "my-key-123")
	rr := httptest.NewRecorder()
	h.Create(rr, req)

	if rr.Code != http.StatusCreated {
		t.Errorf("expected 201, got %d", rr.Code)
	}
	if receivedKey != "my-key-123" {
		t.Errorf("expected idempotency key 'my-key-123', got '%s'", receivedKey)
	}
}

// --- CreateBatch tests ---

func TestCreateBatch_Success(t *testing.T) {
	n := newTestNotification()
	batchID := uuid.New()
	h := NewNotificationHandler(&mockService{
		createBatchFn: func(ctx context.Context, req domain.BatchCreateRequest) (*uuid.UUID, []*domain.Notification, error) {
			return &batchID, []*domain.Notification{n}, nil
		},
	})

	body := `{"notifications":[{"recipient":"+905551234567","channel":"sms","content":"hello","priority":"normal"}]}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/notifications/batch", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.CreateBatch(rr, req)

	if rr.Code != http.StatusCreated {
		t.Errorf("expected 201, got %d", rr.Code)
	}

	var resp domain.APIResponse
	json.NewDecoder(rr.Body).Decode(&resp)
	if !resp.Success {
		t.Error("expected success=true")
	}
}

func TestCreateBatch_InvalidBody(t *testing.T) {
	h := NewNotificationHandler(&mockService{})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/notifications/batch", bytes.NewBufferString("not json"))
	rr := httptest.NewRecorder()
	h.CreateBatch(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}
}

func TestCreateBatch_ValidationError(t *testing.T) {
	h := NewNotificationHandler(&mockService{
		createBatchFn: func(ctx context.Context, req domain.BatchCreateRequest) (*uuid.UUID, []*domain.Notification, error) {
			return nil, nil, fmt.Errorf("validation: at least one notification is required")
		},
	})

	body := `{"notifications":[]}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/notifications/batch", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.CreateBatch(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}
}

func TestCreateBatch_InternalError(t *testing.T) {
	h := NewNotificationHandler(&mockService{
		createBatchFn: func(ctx context.Context, req domain.BatchCreateRequest) (*uuid.UUID, []*domain.Notification, error) {
			return nil, nil, fmt.Errorf("redis connection failed")
		},
	})

	body := `{"notifications":[{"recipient":"+905551234567","channel":"sms","content":"hello","priority":"normal"}]}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/notifications/batch", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.CreateBatch(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rr.Code)
	}
}

func TestCreateBatch_MultipleNotifications(t *testing.T) {
	n1 := newTestNotification()
	n2 := newTestNotification()
	batchID := uuid.New()
	h := NewNotificationHandler(&mockService{
		createBatchFn: func(ctx context.Context, req domain.BatchCreateRequest) (*uuid.UUID, []*domain.Notification, error) {
			return &batchID, []*domain.Notification{n1, n2}, nil
		},
	})

	body := `{"notifications":[{"recipient":"+905551234567","channel":"sms","content":"hello1","priority":"normal"},{"recipient":"+905551234568","channel":"sms","content":"hello2","priority":"high"}]}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/notifications/batch", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.CreateBatch(rr, req)

	if rr.Code != http.StatusCreated {
		t.Errorf("expected 201, got %d", rr.Code)
	}
}

// --- GetByID tests ---

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

func TestGetByID_InternalError(t *testing.T) {
	h := NewNotificationHandler(&mockService{
		getByIDFn: func(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
			return nil, fmt.Errorf("redis timeout")
		},
	})

	r := chi.NewRouter()
	r.Get("/api/v1/notifications/{id}", h.GetByID)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications/"+uuid.New().String(), nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rr.Code)
	}
}

// --- GetByBatchID tests ---

func TestGetByBatchID_Success(t *testing.T) {
	n := newTestNotification()
	h := NewNotificationHandler(&mockService{
		getByBatchFn: func(ctx context.Context, batchID uuid.UUID) ([]*domain.Notification, error) {
			return []*domain.Notification{n}, nil
		},
	})

	r := chi.NewRouter()
	r.Get("/api/v1/notifications/batch/{batchId}", h.GetByBatchID)

	batchID := uuid.New()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications/batch/"+batchID.String(), nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}

	var resp domain.APIResponse
	json.NewDecoder(rr.Body).Decode(&resp)
	if !resp.Success {
		t.Error("expected success=true")
	}
}

func TestGetByBatchID_InvalidBatchID(t *testing.T) {
	h := NewNotificationHandler(&mockService{})

	r := chi.NewRouter()
	r.Get("/api/v1/notifications/batch/{batchId}", h.GetByBatchID)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications/batch/not-a-uuid", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}
}

func TestGetByBatchID_InternalError(t *testing.T) {
	h := NewNotificationHandler(&mockService{
		getByBatchFn: func(ctx context.Context, batchID uuid.UUID) ([]*domain.Notification, error) {
			return nil, fmt.Errorf("redis connection failed")
		},
	})

	r := chi.NewRouter()
	r.Get("/api/v1/notifications/batch/{batchId}", h.GetByBatchID)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications/batch/"+uuid.New().String(), nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rr.Code)
	}
}

func TestGetByBatchID_EmptyResult(t *testing.T) {
	h := NewNotificationHandler(&mockService{
		getByBatchFn: func(ctx context.Context, batchID uuid.UUID) ([]*domain.Notification, error) {
			return []*domain.Notification{}, nil
		},
	})

	r := chi.NewRouter()
	r.Get("/api/v1/notifications/batch/{batchId}", h.GetByBatchID)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications/batch/"+uuid.New().String(), nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
}

// --- Cancel tests ---

func TestCancel_Success(t *testing.T) {
	h := NewNotificationHandler(&mockService{
		cancelFn: func(ctx context.Context, id uuid.UUID) error {
			return nil
		},
	})

	r := chi.NewRouter()
	r.Patch("/api/v1/notifications/{id}/cancel", h.Cancel)

	req := httptest.NewRequest(http.MethodPatch, "/api/v1/notifications/"+uuid.New().String()+"/cancel", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}

	var resp domain.APIResponse
	json.NewDecoder(rr.Body).Decode(&resp)
	if !resp.Success {
		t.Error("expected success=true")
	}
}

func TestCancel_InvalidID(t *testing.T) {
	h := NewNotificationHandler(&mockService{})

	r := chi.NewRouter()
	r.Patch("/api/v1/notifications/{id}/cancel", h.Cancel)

	req := httptest.NewRequest(http.MethodPatch, "/api/v1/notifications/not-a-uuid/cancel", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}
}

func TestCancel_NotFound(t *testing.T) {
	h := NewNotificationHandler(&mockService{
		cancelFn: func(ctx context.Context, id uuid.UUID) error {
			return fmt.Errorf("notification not found")
		},
	})

	r := chi.NewRouter()
	r.Patch("/api/v1/notifications/{id}/cancel", h.Cancel)

	req := httptest.NewRequest(http.MethodPatch, "/api/v1/notifications/"+uuid.New().String()+"/cancel", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rr.Code)
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

func TestCancel_ConcurrentConflict(t *testing.T) {
	h := NewNotificationHandler(&mockService{
		cancelFn: func(ctx context.Context, id uuid.UUID) error {
			return fmt.Errorf("notification was modified concurrently")
		},
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

func TestCancel_InternalError(t *testing.T) {
	h := NewNotificationHandler(&mockService{
		cancelFn: func(ctx context.Context, id uuid.UUID) error {
			return fmt.Errorf("redis timeout")
		},
	})

	r := chi.NewRouter()
	r.Patch("/api/v1/notifications/{id}/cancel", h.Cancel)

	req := httptest.NewRequest(http.MethodPatch, "/api/v1/notifications/"+uuid.New().String()+"/cancel", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rr.Code)
	}
}

// --- List tests ---

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

func TestList_WithAllQueryParams(t *testing.T) {
	n := newTestNotification()
	cursor := uuid.New()
	h := NewNotificationHandler(&mockService{
		listFn: func(ctx context.Context, req domain.ListNotificationsRequest) ([]*domain.Notification, int64, error) {
			if req.Status == nil || *req.Status != "pending" {
				return nil, 0, fmt.Errorf("expected status=pending")
			}
			if req.Channel == nil || *req.Channel != "sms" {
				return nil, 0, fmt.Errorf("expected channel=sms")
			}
			if req.StartDate == nil {
				return nil, 0, fmt.Errorf("expected start_date")
			}
			if req.EndDate == nil {
				return nil, 0, fmt.Errorf("expected end_date")
			}
			if req.Cursor == nil || *req.Cursor != cursor {
				return nil, 0, fmt.Errorf("expected cursor=%s", cursor.String())
			}
			if req.Limit != 50 {
				return nil, 0, fmt.Errorf("expected limit=50, got %d", req.Limit)
			}
			return []*domain.Notification{n}, 1, nil
		},
	})

	url := fmt.Sprintf("/api/v1/notifications?status=pending&channel=sms&start_date=2024-01-01T00:00:00Z&end_date=2024-12-31T23:59:59Z&cursor=%s&limit=50", cursor.String())
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rr := httptest.NewRecorder()
	h.List(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d; body: %s", rr.Code, rr.Body.String())
	}
}

func TestList_InvalidStartDate(t *testing.T) {
	h := NewNotificationHandler(&mockService{})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications?start_date=not-a-date", nil)
	rr := httptest.NewRecorder()
	h.List(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}
}

func TestList_InvalidEndDate(t *testing.T) {
	h := NewNotificationHandler(&mockService{})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications?end_date=invalid", nil)
	rr := httptest.NewRecorder()
	h.List(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}
}

func TestList_InvalidCursor(t *testing.T) {
	h := NewNotificationHandler(&mockService{})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications?cursor=not-a-uuid", nil)
	rr := httptest.NewRecorder()
	h.List(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}
}

func TestList_InvalidLimit_NotANumber(t *testing.T) {
	h := NewNotificationHandler(&mockService{})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications?limit=abc", nil)
	rr := httptest.NewRecorder()
	h.List(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}
}

func TestList_InvalidLimit_Zero(t *testing.T) {
	h := NewNotificationHandler(&mockService{})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications?limit=0", nil)
	rr := httptest.NewRecorder()
	h.List(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}
}

func TestList_InvalidLimit_TooLarge(t *testing.T) {
	h := NewNotificationHandler(&mockService{})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications?limit=101", nil)
	rr := httptest.NewRecorder()
	h.List(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}
}

func TestList_InternalError(t *testing.T) {
	h := NewNotificationHandler(&mockService{
		listFn: func(ctx context.Context, req domain.ListNotificationsRequest) ([]*domain.Notification, int64, error) {
			return nil, 0, fmt.Errorf("redis timeout")
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications", nil)
	rr := httptest.NewRecorder()
	h.List(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rr.Code)
	}
}

func TestList_PaginationWithNextCursor(t *testing.T) {
	// Return limit+1 items to trigger next cursor pagination
	notifications := make([]*domain.Notification, 21)
	for i := range notifications {
		notifications[i] = newTestNotification()
	}

	h := NewNotificationHandler(&mockService{
		listFn: func(ctx context.Context, req domain.ListNotificationsRequest) ([]*domain.Notification, int64, error) {
			return notifications, 100, nil
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications?limit=20", nil)
	rr := httptest.NewRecorder()
	h.List(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}

	var resp domain.APIResponse
	json.NewDecoder(rr.Body).Decode(&resp)
	if !resp.Success {
		t.Error("expected success=true")
	}
}

func TestList_DefaultLimit(t *testing.T) {
	var receivedLimit int
	n := newTestNotification()
	h := NewNotificationHandler(&mockService{
		listFn: func(ctx context.Context, req domain.ListNotificationsRequest) ([]*domain.Notification, int64, error) {
			receivedLimit = req.Limit
			return []*domain.Notification{n}, 1, nil
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications", nil)
	rr := httptest.NewRecorder()
	h.List(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
	if receivedLimit != 20 {
		t.Errorf("expected default limit 20, got %d", receivedLimit)
	}
}

func TestList_ChannelFilter(t *testing.T) {
	var receivedChannel string
	n := newTestNotification()
	h := NewNotificationHandler(&mockService{
		listFn: func(ctx context.Context, req domain.ListNotificationsRequest) ([]*domain.Notification, int64, error) {
			if req.Channel != nil {
				receivedChannel = *req.Channel
			}
			return []*domain.Notification{n}, 1, nil
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications?channel=email", nil)
	rr := httptest.NewRecorder()
	h.List(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
	if receivedChannel != "email" {
		t.Errorf("expected channel=email, got %s", receivedChannel)
	}
}

func TestList_EmptyResult(t *testing.T) {
	h := NewNotificationHandler(&mockService{
		listFn: func(ctx context.Context, req domain.ListNotificationsRequest) ([]*domain.Notification, int64, error) {
			return []*domain.Notification{}, 0, nil
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/notifications", nil)
	rr := httptest.NewRecorder()
	h.List(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
}
