package kafka

import (
	"github.com/IBM/sarama"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/go-kratos/kratos/v2/transport"
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
	// 创建一个链式的中间件组合
	interceptor := middleware.Chain(h.middlewares...)
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				log.Infof("message channel was closed")
				return nil
			}
			log.Infof("Message claimed: value = %s, timestamp = %v, topic = %s", string(msg.Value), msg.Timestamp, msg.Topic)
			//将kafka的header设置进Transport Header中
			t := NewKafkaTransport(msg)
			ctx := transport.NewServerContext(session.Context(), t)
			// 调用用户提供的消息处理函数
			if err := h.handler(ctx, interceptor, msg); err != nil {
				log.Errorf("Error processing message: %v", err)
			} else {
				// 标记消息已处理
				session.MarkMessage(msg, "")
			}
		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/IBM/sarama/issues/1192
		case <-session.Context().Done():
			return nil
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
// must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (h *MultipleConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// 创建一个链式的中间件组合
	interceptor := middleware.Chain(h.middlewares...)
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/IBM/sarama/blob/main/consumer_group.go#L27-L29
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				log.Infof("message channel was closed")
				return nil
			}
			log.Infof("Message claimed: value = %s, timestamp = %v, topic = %s", string(msg.Value), msg.Timestamp, msg.Topic)
			ctx := transport.NewServerContext(session.Context(), &KafkaTransport{
				endpointStr:  msg.Topic,
				operationStr: msg.Topic,
				reqHeader:    Header{},
				replyHeader:  Header{},
			})
			// 调用用户提供的消息处理函数
			if err := h.handler(ctx, interceptor, msg); err != nil {
				log.Errorf("Error processing message: %v", err)
			} else {
				// 标记消息已处理
				session.MarkMessage(msg, "")
			}
		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/IBM/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
	return nil
}
