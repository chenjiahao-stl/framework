package kafka

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"log"
	"os"
	"os/signal"
	"syscall"
	"testing"
)

func TestRun(t *testing.T) {
	// Kafka 配置
	cfg := ConsumerConfig{
		Brokers:       []string{"10.7.4.75:9092", "10.7.4.76:9092", "10.7.4.77:9092"},
		ConsumerGroup: "example-group",
		Topics:        []string{"test-topic"},
		Version:       "2.6.0",
		HandlerFunc: func(ctx context.Context, msg *sarama.ConsumerMessage) error {
			fmt.Printf("Message consumed: key=%s, value=%s\n", string(msg.Key), string(msg.Value))
			return nil // 返回错误时可用于重试逻辑
		},
	}

	// 创建 Kafka 消费者
	consumer, err := NewKafkaConsumer(cfg)
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}

	// 启动消费者
	if err := consumer.Start(); err != nil {
		log.Fatalf("Failed to start Kafka consumer: %v", err)
	}
	log.Println("Kafka consumer started")

	// 等待退出信号
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs

	// 停止消费者
	consumer.Stop()
	log.Println("Kafka consumer stopped")
}
