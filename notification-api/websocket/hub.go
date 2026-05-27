package websocket

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/sertacyildirim/notification-system/shared/domain"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = 30 * time.Second
	defaultMaxConn = 1000
)

type StatusUpdate struct {
	NotificationID uuid.UUID     `json:"notification_id"`
	Status         domain.Status `json:"status"`
}

type client struct {
	conn *websocket.Conn
	send chan []byte
}

type Hub struct {
	mu             sync.RWMutex
	clients        map[*client]bool
	logger         *slog.Logger
	maxClients     int
	allowedOrigins []string
}

func NewHub(logger *slog.Logger, allowedOrigins []string) *Hub {
	maxConn := defaultMaxConn
	return &Hub{
		clients:        make(map[*client]bool),
		logger:         logger,
		maxClients:     maxConn,
		allowedOrigins: allowedOrigins,
	}
}

func (h *Hub) checkOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true // non-browser clients
	}

	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	host := u.Hostname()

	// Always allow localhost
	if host == "localhost" || host == "127.0.0.1" || host == "::1" {
		return true
	}

	for _, allowed := range h.allowedOrigins {
		if origin == allowed {
			return true
		}
	}

	return false
}

// HandleWS upgrades the HTTP connection to a WebSocket for real-time notification status updates.
// @Summary WebSocket for real-time status updates
// @Description Upgrades to WebSocket connection. Server pushes JSON messages: {"notification_id":"uuid","status":"delivered|failed|processing"}. Max 1000 concurrent connections. Ping/pong heartbeat every 30s.
// @Tags websocket
// @Success 101 {string} string "Switching Protocols"
// @Failure 503 {string} string "too many connections"
// @Router /ws [get]
func (h *Hub) HandleWS(w http.ResponseWriter, r *http.Request) {
	// Check connection limit
	h.mu.RLock()
	count := len(h.clients)
	h.mu.RUnlock()

	if count >= h.maxClients {
		h.logger.Warn("websocket connection rejected: max clients reached", "max", h.maxClients)
		http.Error(w, "too many connections", http.StatusServiceUnavailable)
		return
	}

	upgrader := websocket.Upgrader{
		CheckOrigin: h.checkOrigin,
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.logger.Error("websocket upgrade failed", "error", err)
		return
	}

	c := &client{
		conn: conn,
		send: make(chan []byte, 64),
	}

	h.mu.Lock()
	h.clients[c] = true
	h.mu.Unlock()

	h.logger.Debug("websocket client connected", "remote_addr", conn.RemoteAddr(), "clients", count+1)

	go h.writePump(c)
	go h.readPump(c)
}

func (h *Hub) readPump(c *client) {
	defer func() {
		h.removeClient(c)
	}()

	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		if _, _, err := c.conn.ReadMessage(); err != nil {
			break
		}
	}
}

func (h *Hub) writePump(c *client) {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case msg, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (h *Hub) removeClient(c *client) {
	h.mu.Lock()
	if _, ok := h.clients[c]; ok {
		delete(h.clients, c)
		close(c.send)
	}
	h.mu.Unlock()
}

func (h *Hub) Broadcast(notificationID uuid.UUID, status domain.Status) {
	update := StatusUpdate{
		NotificationID: notificationID,
		Status:         status,
	}

	data, err := json.Marshal(update)
	if err != nil {
		return
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	for c := range h.clients {
		select {
		case c.send <- data:
		default:
			h.logger.Debug("websocket client send buffer full, dropping message")
		}
	}
}
