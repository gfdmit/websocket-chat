package config

import (
	"os"
	"websocket-kafka-chat/pkg/logger"

	"github.com/joho/godotenv"
)

type Config struct {
	Server   ServerConfig
	Kafka    KafkaConfig
	LogLevel string
}

type ServerConfig struct {
	Address string
}

type KafkaConfig struct {
	Broker string
	Topic  string
}

func Load() *Config {
	if err := godotenv.Load(); err != nil {
		logger.Warn(".env not found: %v", err)
	}

	return &Config{
		Server: ServerConfig{
			Address: getEnv("SERVER_ADDRESS", ":3000"),
		},
		Kafka: KafkaConfig{
			Broker: getEnv("KAFKA_BROKER", "localhost:9092"),
			Topic:  getEnv("KAFKA_TOPIC", "chat"),
		},
		LogLevel: getEnv("LOG_LEVEL", "info"),
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
