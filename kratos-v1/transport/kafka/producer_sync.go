package kafka

import (
	"context"
	"github.com/IBM/sarama"
	"github.com/chenjiahao-stl/framework/conf"
	"github.com/chenjiahao-stl/framework/logger"
	"github.com/chenjiahao-stl/framework/netutil"
	"log"
)

type SyncProducer struct {
	producer sarama.SyncProducer
	l        *logger.Helper[logger.BusinessData]
}

func NewSyncProducerWithConfig(config *conf.Data_Kafka) (*SyncProducer, func(), error) {
	if len(config.Addrs) == 0 {
		config.Addrs = netutil.JoinHostPort(config.GetHost(), config.GetPort())
	}
	p, err := newSyncProducer(config.Addrs)
	if err != nil {
		return nil, nil, err
	}
	return p, func() {
		p.close()
	}, nil
}

func newSyncProducer(addrs []string, opts ...ClientConfigOption) (*SyncProducer, error) {
	// 配置 Kafka 生产者
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll        // 等待所有分区副本确认
	config.Producer.Partitioner = sarama.NewHashPartitioner // 分区策略
	config.Producer.Return.Successes = true                 // 返回成功的消息

	for _, opt := range opts {
		opt(config)
	}
	producer, err := sarama.NewSyncProducer(addrs, config)
	if err != nil {
		return nil, err
	}
	return &SyncProducer{
		producer: producer,
		l:        logger.NewHelper[logger.BusinessData]("SyncProducer"),
	}, nil
}

func (p *SyncProducer) close() error {
	return p.producer.Close()
}

func (p *SyncProducer) SendMessage(ctx context.Context, msg *sarama.ProducerMessage) error {
	// 发送消息
	_, _, err := p.producer.SendMessage(msg)
	if err != nil {
		log.Printf("Failed to send message: %v", err)
		return err
	}

	log.Printf("Message sent successfully to topic %s with key %s", msg.Topic, msg.Key)
	return nil
}
