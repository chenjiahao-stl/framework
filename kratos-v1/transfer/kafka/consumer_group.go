package kafka

import (
	"context"
	"github.com/IBM/sarama"
	"github.com/go-kratos/kratos/v2/log"
)

type SingleConsumerGroupHandler struct {
	*ConsumerRouter
}

// Setup 初始化
func (h *SingleConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup 清理
func (h *SingleConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim 消费消息
func (h *SingleConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		log.Infof("Message received: topic=%s partition=%d offset=%d key=%s value=%s",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))

		// 调用用户提供的消息处理函数
		if err := h.handler(context.Background(), msg); err != nil {
			log.Errorf("Error processing message: %v", err)
		} else {
			// 标记消息已处理
			session.MarkMessage(msg, "")
		}
	}
	return nil
}

type MultipleConsumerGroupHandler struct {
	*ConsumerRouter
}

// Setup 初始化
func (h *MultipleConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup 清理
func (h *MultipleConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim 消费消息
func (h *MultipleConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		log.Infof("Message received: topic=%s partition=%d offset=%d key=%s value=%s",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))

		// 调用用户提供的消息处理函数
		if err := h.handler(context.Background(), msg); err != nil {
			log.Errorf("Error processing message: %v", err)
		} else {
			// 标记消息已处理
			session.MarkMessage(msg, "")
		}
	}
	return nil
}
