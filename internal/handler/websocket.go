package handler

import (
	"encoding/json"

	"websocket-kafka-chat/internal/domain"
	"websocket-kafka-chat/internal/kafka"
	"websocket-kafka-chat/internal/manager"
	"websocket-kafka-chat/pkg/logger"

	"github.com/gofiber/websocket/v2"
)

type WebSocketHandler struct {
	connManager *manager.ConnectionManager
	producer    *kafka.Producer
}

func NewWebSocketHandler(connManager *manager.ConnectionManager, producer *kafka.Producer) *WebSocketHandler {
	return &WebSocketHandler{
		connManager: connManager,
		producer:    producer,
	}
}

func (h *WebSocketHandler) Handle(c *websocket.Conn) {
	token := c.Query("token")
	if token == "" {
		if err := c.WriteMessage(websocket.TextMessage, []byte(`{"error":"token required"}`)); err != nil {
			logger.Error("Write message error: %v", err)
		}
		if err := c.Close(); err != nil {
			logger.Error("Failed conn close: %v", err)
		}
		return
	}

	client := &manager.ClientConnection{
		UserID: token,
		Conn:   c,
		Send:   make(chan *domain.Message, 256),
	}

	h.connManager.Register(client)
	defer func() {
		h.connManager.Unregister(client)
		if err := c.Close(); err != nil {
			logger.Error("Failed conn close: %v", err)
		}
	}()

	go h.writePump(client)

	h.readPump(client)
}

func (h *WebSocketHandler) readPump(client *manager.ClientConnection) {
	for {
		var msg domain.Message
		if err := client.Conn.ReadJSON(&msg); err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				logger.Error("WebSocket error: %v", err)
			}
			break
		}

		msg.From = client.UserID
		msg.Timestamp = domain.NewMessage("", "", "", "").Timestamp
		if msg.ID == "" {
			msg.ID = domain.NewMessage(client.UserID, "", "", "").ID
		}

		if err := h.producer.Publish(&msg); err != nil {
			logger.Error("Kafka publish error: %v", err)
			errorMsg := domain.NewMessage("system", client.UserID, "Failed to send", "error")
			client.Send <- errorMsg
		}
	}
}

func (h *WebSocketHandler) writePump(client *manager.ClientConnection) {
	for msg := range client.Send {
		data, err := json.Marshal(msg)
		if err != nil {
			logger.Error("Marshal error: %v", err)
			continue
		}

		if err := client.Conn.WriteMessage(websocket.TextMessage, data); err != nil {
			logger.Error("Write error: %v", err)
			return
		}
	}
}
