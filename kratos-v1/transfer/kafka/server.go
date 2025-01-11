package kafka

import (
	"context"
	"github.com/IBM/sarama"
	"log"
	"sync"
)

// ConsumerConfig 消费者配置
type ConsumerConfig struct {
	Brokers       []string                                             // Kafka broker 地址列表
	ConsumerGroup string                                               // 消费者组名称
	Topics        []string                                             // 订阅的主题
	Version       string                                               // Kafka 版本
	HandlerFunc   func(context.Context, *sarama.ConsumerMessage) error // 消息处理函数
}

// KafkaConsumer Kafka 消费者工具类
type KafkaConsumer struct {
	config ConsumerConfig
	client sarama.ConsumerGroup
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewKafkaConsumer 创建一个 Kafka 消费者
func NewKafkaConsumer(cfg ConsumerConfig) (*KafkaConsumer, error) {
	version, err := sarama.ParseKafkaVersion(cfg.Version)
	if err != nil {
		return nil, err
	}

	// Sarama 配置
	config := sarama.NewConfig()
	config.Version = version
	config.Consumer.Offsets.Initial = sarama.OffsetNewest // 默认从最新的消息开始消费
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin

	// 创建消费者组
	client, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.ConsumerGroup, config)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &KafkaConsumer{
		config: cfg,
		client: client,
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

// Start 开始消费
func (kc *KafkaConsumer) Start() error {
	kc.wg.Add(1)
	go func() {
		defer kc.wg.Done()
		for {
			if err := kc.client.Consume(kc.ctx, kc.config.Topics, kc); err != nil {
				log.Printf("Kafka consume error: %v", err)
			}
			// 如果 context 被取消，则退出
			if kc.ctx.Err() != nil {
				return
			}
		}
	}()
	return nil
}

// Stop 停止消费
func (kc *KafkaConsumer) Stop() {
	kc.cancel()
	kc.wg.Wait()
	if err := kc.client.Close(); err != nil {
		log.Printf("Error closing Kafka consumer: %v", err)
	}
}

// 实现 ConsumerGroupHandler 接口
// Setup 初始化
func (kc *KafkaConsumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup 清理
func (kc *KafkaConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim 消费消息
func (kc *KafkaConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		log.Printf("Message received: topic=%s partition=%d offset=%d key=%s value=%s",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))

		// 调用用户提供的消息处理函数
		if err := kc.config.HandlerFunc(context.Background(), msg); err != nil {
			log.Printf("Error processing message: %v", err)
		} else {
			// 标记消息已处理
			session.MarkMessage(msg, "")
		}
	}
	return nil
}
