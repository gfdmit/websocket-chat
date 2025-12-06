package manager

import (
	"fmt"
	"sync"
	"time"

	"websocket-kafka-chat/internal/domain"
	"websocket-kafka-chat/pkg/logger"

	"github.com/gofiber/websocket/v2"
)

type ClientConnection struct {
	UserID string
	Conn   *websocket.Conn
	Send   chan *domain.Message
}

type ConnectionManager struct {
	clients    map[string]*ClientConnection
	register   chan *ClientConnection
	unregister chan *ClientConnection
	mu         sync.RWMutex
}

func NewConnectionManager() *ConnectionManager {
	return &ConnectionManager{
		clients:    make(map[string]*ClientConnection),
		register:   make(chan *ClientConnection),
		unregister: make(chan *ClientConnection),
	}
}

func (m *ConnectionManager) Run() {
	for {
		select {
		case client := <-m.register:
			m.mu.Lock()
			m.clients[client.UserID] = client
			m.mu.Unlock()
			logger.Info("Client connected: %s (total: %d)", client.UserID, len(m.clients))

		case client := <-m.unregister:
			m.mu.Lock()
			if _, ok := m.clients[client.UserID]; ok {
				delete(m.clients, client.UserID)
				close(client.Send)
			}
			m.mu.Unlock()
			logger.Info("Client disconnected: %s (total: %d)", client.UserID, len(m.clients))
		}
	}
}

func (m *ConnectionManager) Register(client *ClientConnection) {
	m.register <- client
}

func (m *ConnectionManager) Unregister(client *ClientConnection) {
	m.unregister <- client
}

func (m *ConnectionManager) SendToUser(userID string, msg *domain.Message) error {
	m.mu.RLock()
	client, ok := m.clients[userID]
	m.mu.RUnlock()

	if !ok {
		logger.Warn("User %s not connected", userID)
		return fmt.Errorf("user %s not connected", userID)
	}

	select {
	case client.Send <- msg:
		return nil
	case <-time.After(2 * time.Second):
		logger.Warn("Timeout sending to %s", userID)
		return fmt.Errorf("timeout sending to %s", userID)
	}
}

func (m *ConnectionManager) GetClientCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.clients)
}
