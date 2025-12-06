package kafka

import (
	"context"
	"encoding/json"
	"time"

	"websocket-kafka-chat/internal/config"
	"websocket-kafka-chat/internal/domain"
	"websocket-kafka-chat/pkg/logger"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(cfg config.KafkaConfig) *Producer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Broker),
		Topic:        cfg.Topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		Async:        false,
	}

	return &Producer{writer: writer}
}

func (p *Producer) Publish(msg *domain.Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	logger.Info("Publishing to Kafka: %s -> %s, content: %s", msg.From, msg.To, msg.Content)

	err = p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(msg.To),
		Value: data,
	})

	if err != nil {
		logger.Error("Failed to publish to Kafka: %v", err)
		return err
	}

	logger.Info("Successfully published to Kafka: %s -> %s, content: %s", msg.From, msg.To, msg.Content)
	return nil
}

func (p *Producer) Close() error {
	if p.writer != nil {
		return p.writer.Close()
	}
	return nil
}
