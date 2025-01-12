package kafka

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"testing"
)

func TestRun(t *testing.T) {
	// Kafka 配置
	//cfg := ConsumerConfig{
	//	Brokers:       []string{"192.168.3.120:9192", "192.168.3.120:9292", "192.168.3.120:9392"},
	//	ConsumerGroup: "example-group",
	//	Topics:        []string{"test-topic"},
	//	//Version:       "2.6.0",
	//	HandlerFunc: func(ctx context.Context, msg *sarama.ConsumerMessage) error {
	//		fmt.Printf("Message consumed: key=%s, value=%s\n", string(msg.Key), string(msg.Value))
	//		return nil // 返回错误时可用于重试逻辑
	//	},
	//}

	// 创建 Kafka 消费者
	consumer, err := NewKafkaServer()
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}

	// 启动消费者
	if err := consumer.Start(context.Background()); err != nil {
		log.Fatalf("Failed to start Kafka consumer: %v", err)
	}
	log.Println("Kafka consumer started")

	// 等待退出信号
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs

	// 停止消费者
	consumer.Stop(context.Background())
	log.Println("Kafka consumer stopped")
}
