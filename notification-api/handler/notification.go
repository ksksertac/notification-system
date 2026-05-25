package handler

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/sertacyildirim/notification-system/notification-api/middleware"
	"github.com/sertacyildirim/notification-system/shared/domain"
	"github.com/sertacyildirim/notification-system/notification-api/service"
)

type NotificationHandler struct {
	svc service.NotificationService
}

func NewNotificationHandler(svc service.NotificationService) *NotificationHandler {
	return &NotificationHandler{svc: svc}
}

// Create godoc
// @Summary Create a notification
// @Description Create a new notification for delivery via SMS, Email, or Push
// @Tags notifications
// @Accept json
// @Produce json
// @Param Idempotency-Key header string false "Idempotency key for deduplication"
// @Param request body domain.CreateNotificationRequest true "Notification request"
// @Success 201 {object} domain.APIResponse{data=domain.NotificationResponse}
// @Failure 400 {object} domain.APIResponse{error=domain.APIError}
// @Failure 500 {object} domain.APIResponse{error=domain.APIError}
// @Router /notifications [post]
func (h *NotificationHandler) Create(w http.ResponseWriter, r *http.Request) {
	var req domain.CreateNotificationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, r, http.StatusBadRequest, "INVALID_REQUEST", "invalid request body")
		return
	}

	idempotencyKey := r.Header.Get("Idempotency-Key")

	n, err := h.svc.Create(r.Context(), req, idempotencyKey)
	if err != nil {
		if strings.Contains(err.Error(), "validation:") {
			writeError(w, r, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
			return
		}
		writeError(w, r, http.StatusInternalServerError, "CREATE_FAILED", "failed to create notification")
		return
	}

	writeSuccess(w, r, http.StatusCreated, domain.ToNotificationResponse(n))
}

// CreateBatch godoc
// @Summary Create a batch of notifications
// @Description Create up to 1000 notifications in a single request
// @Tags notifications
// @Accept json
// @Produce json
// @Param request body domain.BatchCreateRequest true "Batch notification request"
// @Success 201 {object} domain.APIResponse{data=domain.BatchCreateResponse}
// @Failure 400 {object} domain.APIResponse{error=domain.APIError}
// @Failure 500 {object} domain.APIResponse{error=domain.APIError}
// @Router /notifications/batch [post]
func (h *NotificationHandler) CreateBatch(w http.ResponseWriter, r *http.Request) {
	var req domain.BatchCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, r, http.StatusBadRequest, "INVALID_REQUEST", "invalid request body")
		return
	}

	batchID, notifications, err := h.svc.CreateBatch(r.Context(), req)
	if err != nil && batchID == nil {
		if strings.Contains(err.Error(), "validation:") {
			writeError(w, r, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
			return
		}
		writeError(w, r, http.StatusInternalServerError, "BATCH_CREATE_FAILED", "failed to create batch")
		return
	}

	responses := make([]domain.NotificationResponse, len(notifications))
	for i, n := range notifications {
		responses[i] = domain.ToNotificationResponse(n)
	}

	writeSuccess(w, r, http.StatusCreated, domain.BatchCreateResponse{
		BatchID:       *batchID,
		Total:         len(notifications),
		Notifications: responses,
	})
}

// GetByID godoc
// @Summary Get notification by ID
// @Description Retrieve a single notification by its UUID
// @Tags notifications
// @Produce json
// @Param id path string true "Notification UUID"
// @Success 200 {object} domain.APIResponse{data=domain.NotificationResponse}
// @Failure 400 {object} domain.APIResponse{error=domain.APIError}
// @Failure 404 {object} domain.APIResponse{error=domain.APIError}
// @Failure 500 {object} domain.APIResponse{error=domain.APIError}
// @Router /notifications/{id} [get]
func (h *NotificationHandler) GetByID(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "INVALID_ID", "invalid notification ID")
		return
	}

	n, err := h.svc.GetByID(r.Context(), id)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "GET_FAILED", "failed to get notification")
		return
	}
	if n == nil {
		writeError(w, r, http.StatusNotFound, "NOT_FOUND", "notification not found")
		return
	}

	writeSuccess(w, r, http.StatusOK, domain.ToNotificationResponse(n))
}

// GetByBatchID godoc
// @Summary Get notifications by batch ID
// @Description Retrieve all notifications belonging to a batch
// @Tags notifications
// @Produce json
// @Param batchId path string true "Batch UUID"
// @Success 200 {object} domain.APIResponse{data=[]domain.NotificationResponse}
// @Failure 400 {object} domain.APIResponse{error=domain.APIError}
// @Failure 500 {object} domain.APIResponse{error=domain.APIError}
// @Router /notifications/batch/{batchId} [get]
func (h *NotificationHandler) GetByBatchID(w http.ResponseWriter, r *http.Request) {
	batchID, err := uuid.Parse(chi.URLParam(r, "batchId"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "INVALID_ID", "invalid batch ID")
		return
	}

	notifications, err := h.svc.GetByBatchID(r.Context(), batchID)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "GET_FAILED", "failed to get batch notifications")
		return
	}

	responses := make([]domain.NotificationResponse, len(notifications))
	for i, n := range notifications {
		responses[i] = domain.ToNotificationResponse(n)
	}

	writeSuccess(w, r, http.StatusOK, responses)
}

// Cancel godoc
// @Summary Cancel a notification
// @Description Cancel a pending or queued notification before delivery
// @Tags notifications
// @Produce json
// @Param id path string true "Notification UUID"
// @Success 200 {object} domain.APIResponse
// @Failure 400 {object} domain.APIResponse{error=domain.APIError}
// @Failure 404 {object} domain.APIResponse{error=domain.APIError}
// @Failure 409 {object} domain.APIResponse{error=domain.APIError}
// @Failure 500 {object} domain.APIResponse{error=domain.APIError}
// @Router /notifications/{id}/cancel [patch]
func (h *NotificationHandler) Cancel(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "INVALID_ID", "invalid notification ID")
		return
	}

	err = h.svc.Cancel(r.Context(), id)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeError(w, r, http.StatusNotFound, "NOT_FOUND", err.Error())
			return
		}
		if strings.Contains(err.Error(), "cannot cancel") {
			writeError(w, r, http.StatusConflict, "CONFLICT", err.Error())
			return
		}
		if strings.Contains(err.Error(), "concurrently") {
			writeError(w, r, http.StatusConflict, "CONFLICT", err.Error())
			return
		}
		writeError(w, r, http.StatusInternalServerError, "CANCEL_FAILED", "failed to cancel notification")
		return
	}

	writeSuccess(w, r, http.StatusOK, map[string]string{"status": "cancelled"})
}

// List godoc
// @Summary List notifications
// @Description List notifications with cursor-based pagination and optional filters
// @Tags notifications
// @Produce json
// @Param status query string false "Filter by status (pending, queued, processing, delivered, failed, cancelled)"
// @Param channel query string false "Filter by channel (sms, email, push)"
// @Param start_date query string false "Filter by start date (RFC3339)"
// @Param end_date query string false "Filter by end date (RFC3339)"
// @Param cursor query string false "Cursor for pagination (notification UUID)"
// @Param limit query int false "Page size (1-100, default 20)"
// @Success 200 {object} domain.APIResponse{data=domain.ListNotificationsResponse}
// @Failure 400 {object} domain.APIResponse{error=domain.APIError}
// @Failure 500 {object} domain.APIResponse{error=domain.APIError}
// @Router /notifications [get]
func (h *NotificationHandler) List(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	req := domain.ListNotificationsRequest{
		Limit: 20,
	}

	if v := q.Get("status"); v != "" {
		req.Status = &v
	}
	if v := q.Get("channel"); v != "" {
		req.Channel = &v
	}
	if v := q.Get("start_date"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, "INVALID_DATE", "invalid start_date format (use RFC3339)")
			return
		}
		req.StartDate = &t
	}
	if v := q.Get("end_date"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, "INVALID_DATE", "invalid end_date format (use RFC3339)")
			return
		}
		req.EndDate = &t
	}
	if v := q.Get("cursor"); v != "" {
		id, err := uuid.Parse(v)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, "INVALID_CURSOR", "invalid cursor format")
			return
		}
		req.Cursor = &id
	}
	if v := q.Get("limit"); v != "" {
		limit, err := strconv.Atoi(v)
		if err != nil || limit < 1 || limit > 100 {
			writeError(w, r, http.StatusBadRequest, "INVALID_LIMIT", "limit must be between 1 and 100")
			return
		}
		req.Limit = limit
	}

	notifications, total, err := h.svc.List(r.Context(), req)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "LIST_FAILED", "failed to list notifications")
		return
	}

	responses := make([]domain.NotificationResponse, len(notifications))
	for i, n := range notifications {
		responses[i] = domain.ToNotificationResponse(n)
	}

	var nextCursor *uuid.UUID
	if len(notifications) > req.Limit {
		notifications = notifications[:req.Limit]
		responses = responses[:req.Limit]
		id := notifications[len(notifications)-1].ID
		nextCursor = &id
	}

	writeSuccess(w, r, http.StatusOK, domain.ListNotificationsResponse{
		Notifications: responses,
		NextCursor:    nextCursor,
		Total:         total,
	})
}

func writeSuccess(w http.ResponseWriter, r *http.Request, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(domain.APIResponse{
		Success:       true,
		Data:          data,
		CorrelationID: middleware.GetCorrelationID(r.Context()),
	})
}

func writeError(w http.ResponseWriter, r *http.Request, status int, code string, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(domain.APIResponse{
		Success: false,
		Error: &domain.APIError{
			Code:    code,
			Message: message,
		},
		CorrelationID: middleware.GetCorrelationID(r.Context()),
	})
}
