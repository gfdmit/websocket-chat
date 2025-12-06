package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"websocket-kafka-chat/internal/config"
	"websocket-kafka-chat/internal/handler"
	"websocket-kafka-chat/internal/kafka"
	"websocket-kafka-chat/internal/manager"
	"websocket-kafka-chat/pkg/logger"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	fiberLogger "github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/websocket/v2"
)

func main() {
	logger.Init()
	cfg := config.Load()

	connManager := manager.NewConnectionManager()
	go connManager.Run()

	producer := kafka.NewProducer(cfg.Kafka)
	defer producer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	consumer := kafka.NewConsumer(cfg.Kafka, connManager)
	go consumer.Start(ctx)

	app := fiber.New(fiber.Config{
		AppName: "WebSocket Chat",
	})

	app.Use(fiberLogger.New())
	app.Use(cors.New())

	app.Use("/ws", func(c *fiber.Ctx) error {
		if websocket.IsWebSocketUpgrade(c) {
			return c.Next()
		}
		return fiber.ErrUpgradeRequired
	})

	wsHandler := handler.NewWebSocketHandler(connManager, producer)
	app.Get("/ws", websocket.New(wsHandler.Handle))

	app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"status":  "ok",
			"clients": connManager.GetClientCount(),
		})
	})

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		sig := <-sigChan
		logger.Info("Received signal %s, shutting down...", sig)
		cancel()
		if err := producer.Close(); err != nil {
			logger.Error("Error closing producer: %v", err)
		}
		if err := app.Shutdown(); err != nil {
			logger.Error("Error during app shutdown: %v", err)
		}
	}()

	logger.Info("Server started on %s", cfg.Server.Address)
	if err := app.Listen(cfg.Server.Address); err != nil {
		log.Fatal(err)
	}

}
