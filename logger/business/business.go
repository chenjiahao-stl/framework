package business

import (
	"context"
	"github.com/IBM/sarama"
	"github.com/chenjiahao-stl/framework/conf"
	"github.com/chenjiahao-stl/framework/kratos-v1/transport/kafka"
	"github.com/chenjiahao-stl/framework/logger"
)

type kafkaSend struct {
	topic         string
	asyncProducer *kafka.AsyncProducer
	cleanup       func()
}

func NewKafkaSend(conf *conf.Data_Kafka, topic string) logger.NewKafkaSendFunc {
	return func() (logger.BusinessWrite, error) {
		asyncProducer, f, err := kafka.NewAsyncProducer(conf)
		if err != nil {
			return nil, err
		}
		return &kafkaSend{
			topic:         topic,
			asyncProducer: asyncProducer,
			cleanup:       f,
		}, nil
	}
}

func (s *kafkaSend) Write(p []byte) (n int, err error) {
	msg := &sarama.ProducerMessage{
		Topic:   s.topic,
		Headers: make([]sarama.RecordHeader, 0),
		Value:   sarama.ByteEncoder(p),
	}
	return 0, s.asyncProducer.SendMessage(context.Background(), msg)
}
func (s *kafkaSend) Close() error {
	s.cleanup()
	return nil
}
