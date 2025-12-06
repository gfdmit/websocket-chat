package kafka

import (
	"context"
	"encoding/json"
	"time"

	"websocket-kafka-chat/internal/config"
	"websocket-kafka-chat/internal/domain"
	"websocket-kafka-chat/internal/manager"
	"websocket-kafka-chat/pkg/logger"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader      *kafka.Reader
	connManager *manager.ConnectionManager
}

func NewConsumer(cfg config.KafkaConfig, connManager *manager.ConnectionManager) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{cfg.Broker},
		Topic:       cfg.Topic,
		GroupID:     "chat-group",
		StartOffset: kafka.LastOffset,
		MaxWait:     500 * time.Millisecond,
		MinBytes:    1,
		MaxBytes:    10e6,
	})

	return &Consumer{
		reader:      reader,
		connManager: connManager,
	}
}

func (c *Consumer) Start(ctx context.Context) {
	logger.Info("Kafka consumer started, waiting for messages...")

	time.Sleep(2 * time.Second)

	for {
		select {
		case <-ctx.Done():
			logger.Info("Kafka consumer stopped")
			if err := c.reader.Close(); err != nil {
				logger.Error("Failed to close reader: %v", err)
			}
			return
		default:
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				logger.Error("Kafka fetch error: %v", err)
				time.Sleep(time.Second)
				continue
			}

			logger.Info("Received message from Kafka: %s", string(msg.Value))

			var chatMsg domain.Message
			if err := json.Unmarshal(msg.Value, &chatMsg); err != nil {
				logger.Error("Unmarshal error: %v", err)
				if err := c.reader.CommitMessages(ctx, msg); err != nil {
					logger.Error("Failed to commit message: %v", err)
				}
				continue
			}

			logger.Info("Attempting to deliver: %s -> %s", chatMsg.From, chatMsg.To)

			if err := c.connManager.SendToUser(chatMsg.To, &chatMsg); err != nil {
				logger.Error("Failed to deliver to %s: %v", chatMsg.To, err)
			} else {
				logger.Info("Successfully delivered: %s -> %s", chatMsg.From, chatMsg.To)
			}

			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				logger.Error("Failed to commit message: %v", err)
			}
		}
	}
}
