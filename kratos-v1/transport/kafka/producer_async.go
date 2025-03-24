package kafka

import (
	"context"
	"github.com/IBM/sarama"
	"github.com/chenjiahao-stl/framework/conf"
	"github.com/chenjiahao-stl/framework/logger"
	"github.com/chenjiahao-stl/framework/netutil"
	"log"
)

type AsyncProducer struct {
	producer sarama.AsyncProducer
	l        *logger.Helper[logger.BusinessData]
}

func NewAsyncProducer(config *conf.Data_Kafka) (*AsyncProducer, func(), error) {
	if len(config.Addrs) == 0 {
		config.Addrs = netutil.JoinHostPort(config.GetHost(), config.GetPort())
	}
	p, err := newAsyncProducer(config.Addrs)
	if err != nil {
		return nil, nil, err
	}
	return p, func() {
		p.close()
	}, nil
}

func newAsyncProducer(addrs []string, opts ...ClientConfigOption) (*AsyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll        // 等待所有分区副本确认
	config.Producer.Partitioner = sarama.NewHashPartitioner // 分区策略
	config.Producer.Return.Successes = true                 // 返回成功的消息

	asyncProducer, err := sarama.NewAsyncProducer(addrs, config)
	if err != nil {
		return nil, err
	}
	return &AsyncProducer{
		producer: asyncProducer,
		l:        logger.NewHelper[logger.BusinessData]("AsyncProducer"),
	}, nil
}

func (p *AsyncProducer) close() error {
	return p.producer.Close()
}

func (p *AsyncProducer) SendMessage(ctx context.Context, msg *sarama.ProducerMessage) error {
	select {
	case p.producer.Input() <- msg:
		log.Printf("Message sent successfully to topic %s with key %s", msg.Topic, msg.Key)
		return nil
	case err := <-p.producer.Errors():
		log.Printf("Failed to send message: %v", err)
		return err
	}
}

type AsyncBatchProducer struct {
}
