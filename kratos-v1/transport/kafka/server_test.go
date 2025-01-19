package kafka

import (
	"context"
	"encoding/json"
	"github.com/IBM/sarama"
	"github.com/go-kratos/kratos/v2/log"
	"os"
	"os/signal"
	"syscall"
	"testing"
)

type Order struct {
	OrderId string `json:"order_id"`
	Price   string `json:"price"`
	Symbol  string `json:"symbol"`
}

func TestRun(t *testing.T) {
	// 创建 Kafka 消费者
	server, err := NewKafkaServer(WithAddress([]string{"192.168.3.120:9192", "192.168.3.120:9292", "192.168.3.120:9392"}), WithVersion("3.3.1"))
	if err != nil {
		log.Errorf("Failed to create Kafka consumer: %v", err)
	}

	server.ConsumerHandlerWithStrategy("test-topic", func(ctx context.Context, message *sarama.ConsumerMessage) error {
		//这是声明了一个 Order 类型的变量 order，但没有进行初始化，order 会被初始化为 Order 类型的零值。
		var order Order
		if err := json.Unmarshal(message.Value, &order); err != nil {
			return err
		}
		log.Infof("order: %v", order)
		return nil
	}, server.ProcessMessageSequential).SetGroupId("example-group")
	// 启动消费者
	if err := server.Start(context.Background()); err != nil {
		log.Fatalf("Failed to start Kafka consumer: %v", err)
	}
	log.Infof("Kafka consumer started")

	// 等待退出信号
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs

	// 停止消费者
	server.Stop(context.Background())
	log.Infof("Kafka consumer stopped")
}
