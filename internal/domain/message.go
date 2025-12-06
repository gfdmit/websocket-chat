package domain

import (
	"fmt"
	"time"
)

type Message struct {
	ID        string    `json:"id"`
	From      string    `json:"from"`
	To        string    `json:"to"`
	Content   string    `json:"content"`
	Timestamp time.Time `json:"timestamp"`
	Type      string    `json:"type"`
}

func NewMessage(from, to, content, msgType string) *Message {
	return &Message{
		ID:        fmt.Sprintf("%d-%s", time.Now().UnixNano(), from),
		From:      from,
		To:        to,
		Content:   content,
		Type:      msgType,
		Timestamp: time.Now(),
	}
}
