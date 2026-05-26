package websocket

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/sertacyildirim/notification-system/shared/domain"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// --- Origin checking tests ---

func TestCheckOrigin_EmptyOrigin(t *testing.T) {
	hub := NewHub(testLogger(), []string{"https://example.com"})
	req := httptest.NewRequest(http.MethodGet, "/ws", nil)
	// No Origin header set — simulates a non-browser client

	if !hub.checkOrigin(req) {
		t.Error("expected empty origin to be allowed (non-browser client)")
	}
}

func TestCheckOrigin_Localhost(t *testing.T) {
	hub := NewHub(testLogger(), nil) // no explicit allowlist

	cases := []struct {
		name   string
		origin string
	}{
		{"localhost_http", "http://localhost"},
		{"localhost_with_port", "http://localhost:3000"},
		{"127.0.0.1", "http://127.0.0.1"},
		{"127.0.0.1_with_port", "http://127.0.0.1:8080"},
		{"ipv6_loopback", "http://[::1]"},
		{"ipv6_loopback_with_port", "http://[::1]:9090"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/ws", nil)
			req.Header.Set("Origin", tc.origin)

			if !hub.checkOrigin(req) {
				t.Errorf("expected localhost origin %q to be allowed", tc.origin)
			}
		})
	}
}

func TestCheckOrigin_AllowedOrigin(t *testing.T) {
	allowedOrigins := []string{"https://example.com", "https://app.mysite.org"}
	hub := NewHub(testLogger(), allowedOrigins)

	for _, origin := range allowedOrigins {
		t.Run(origin, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/ws", nil)
			req.Header.Set("Origin", origin)

			if !hub.checkOrigin(req) {
				t.Errorf("expected origin %q to be allowed", origin)
			}
		})
	}
}

func TestCheckOrigin_DisallowedOrigin(t *testing.T) {
	hub := NewHub(testLogger(), []string{"https://example.com"})
	req := httptest.NewRequest(http.MethodGet, "/ws", nil)
	req.Header.Set("Origin", "https://evil.com")

	if hub.checkOrigin(req) {
		t.Error("expected disallowed origin to be rejected")
	}
}

func TestCheckOrigin_InvalidURL(t *testing.T) {
	hub := NewHub(testLogger(), []string{"https://example.com"})

	cases := []struct {
		name   string
		origin string
	}{
		{"control_char", string([]byte{0x7f})},
		{"missing_scheme", "://not-a-valid-url"},
		{"bare_string", "not-even-a-url"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/ws", nil)
			req.Header.Set("Origin", tc.origin)

			if hub.checkOrigin(req) {
				t.Errorf("expected malformed origin %q to be rejected", tc.origin)
			}
		})
	}
}

// --- Hub creation tests ---

func TestNewHub(t *testing.T) {
	logger := testLogger()
	origins := []string{"https://example.com"}
	hub := NewHub(logger, origins)

	if hub == nil {
		t.Fatal("expected hub to be non-nil")
	}
	if hub.clients == nil {
		t.Error("expected clients map to be initialized")
	}
	if len(hub.clients) != 0 {
		t.Error("expected clients map to be empty")
	}
	if hub.maxClients != defaultMaxConn {
		t.Errorf("expected maxClients=%d, got %d", defaultMaxConn, hub.maxClients)
	}
	if hub.logger != logger {
		t.Error("expected logger to be set")
	}
	if len(hub.allowedOrigins) != 1 || hub.allowedOrigins[0] != "https://example.com" {
		t.Errorf("expected allowedOrigins=[https://example.com], got %v", hub.allowedOrigins)
	}
}

// --- Connection limit tests ---

func TestHandleWS_ConnectionLimit(t *testing.T) {
	hub := NewHub(testLogger(), nil)
	hub.maxClients = 0 // set to 0 so any connection is over the limit

	server := httptest.NewServer(http.HandlerFunc(hub.HandleWS))
	defer server.Close()

	// Make a plain HTTP request (not a WebSocket upgrade) to trigger the 503 path
	resp, err := http.Get(server.URL)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected status 503, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "too many connections") {
		t.Errorf("expected 'too many connections' in body, got %q", string(body))
	}
}

// --- Broadcast tests ---

func TestBroadcast_SendsToAllClients(t *testing.T) {
	hub := NewHub(testLogger(), nil)

	server := httptest.NewServer(http.HandlerFunc(hub.HandleWS))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws"

	const numClients = 3
	conns := make([]*websocket.Conn, numClients)
	for i := 0; i < numClients; i++ {
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			t.Fatalf("failed to connect client %d: %v", i, err)
		}
		defer conn.Close()
		conns[i] = conn
	}

	// Give the server a moment to register all clients
	time.Sleep(50 * time.Millisecond)

	hub.mu.RLock()
	clientCount := len(hub.clients)
	hub.mu.RUnlock()
	if clientCount != numClients {
		t.Fatalf("expected %d clients registered, got %d", numClients, clientCount)
	}

	// Broadcast a status update
	notifID := uuid.New()
	hub.Broadcast(notifID, domain.StatusDelivered)

	// Verify each client receives the broadcast
	for i, conn := range conns {
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, msg, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("client %d failed to read message: %v", i, err)
		}

		var update StatusUpdate
		if err := json.Unmarshal(msg, &update); err != nil {
			t.Fatalf("client %d: failed to unmarshal message: %v", i, err)
		}
		if update.NotificationID != notifID {
			t.Errorf("client %d: expected notification_id=%s, got %s", i, notifID, update.NotificationID)
		}
		if update.Status != domain.StatusDelivered {
			t.Errorf("client %d: expected status=%s, got %s", i, domain.StatusDelivered, update.Status)
		}
	}
}

func TestBroadcast_FullBuffer(t *testing.T) {
	hub := NewHub(testLogger(), nil)

	// Create a fake client with a full send buffer (capacity 1, already full)
	fullChan := make(chan []byte, 1)
	fullChan <- []byte("blocking")

	blockedClient := &client{
		send: fullChan,
	}

	// Create a normal client with a send buffer that has room
	normalChan := make(chan []byte, 64)
	normalClient := &client{
		send: normalChan,
	}

	hub.mu.Lock()
	hub.clients[blockedClient] = true
	hub.clients[normalClient] = true
	hub.mu.Unlock()

	notifID := uuid.New()

	// This should not block even though one client's buffer is full
	done := make(chan struct{})
	go func() {
		hub.Broadcast(notifID, domain.StatusFailed)
		close(done)
	}()

	select {
	case <-done:
		// Broadcast completed without blocking - good
	case <-time.After(2 * time.Second):
		t.Fatal("Broadcast blocked on full client buffer")
	}

	// Verify the normal client received the message
	select {
	case msg := <-normalChan:
		var update StatusUpdate
		if err := json.Unmarshal(msg, &update); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}
		if update.NotificationID != notifID {
			t.Errorf("expected notification_id=%s, got %s", notifID, update.NotificationID)
		}
		if update.Status != domain.StatusFailed {
			t.Errorf("expected status=%s, got %s", domain.StatusFailed, update.Status)
		}
	default:
		t.Error("expected normal client to receive the broadcast message")
	}

	// Verify the blocked client still has only its original message (broadcast was dropped)
	if len(fullChan) != 1 {
		t.Errorf("expected blocked client buffer to still have 1 message, got %d", len(fullChan))
	}
}
