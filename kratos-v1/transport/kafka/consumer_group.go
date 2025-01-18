package kafka

import (
	"github.com/IBM/sarama"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/go-kratos/kratos/v2/transport"
	"time"
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

	//存储多条msg 再一起消费
	//初始化一个长度为0,并且切片的容量被初始化为 h.batchSize。
	buffer := make([]*sarama.ConsumerMessage, 0, h.batchSize)

	handler := func() error {
		if len(buffer) > 0 {
			ctx := transport.NewServerContext(session.Context(), &KafkaTransport{
				endpointStr:  h.topic,
				operationStr: h.topic,
				reqHeader:    Header{},
				replyHeader:  Header{},
			})
			// 调用用户提供的消息处理函数
			if err := h.handler(ctx, interceptor, buffer...); err != nil {
				log.Errorf("Error processing message: %v", err)
				return err
			} else {
				// 标记消息已处理
				session.MarkMessage(buffer[len(buffer)-1], "")
				if !h.autoCommit {
					session.Context()
				}
				buffer = buffer[:0] //清空切片，保留原有底层数组
				return nil
			}
		}

		return nil
	}

	//定時处理批量消息,要不然的话如果buffer一直未满那么就不会被处理
	ticker := time.NewTicker(h.timeInterval)
	//time.NewTicker 在创建时，会为定时器分配一个底层的定时器资源（一个 goroutine 用于触发定时器事件）。如果不调用 Stop，这个定时器会继续运行下去，浪费系统资源，甚至可能导致内存泄漏。
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := handler(); err != nil {
				return err
			}
		case msg, ok := <-claim.Messages():
			if !ok {
				log.Infof("message channel was closed")
				return nil
			}
			log.Infof("Message claimed: value = %s, timestamp = %v, topic = %s", string(msg.Value), msg.Timestamp, msg.Topic)

			// 调用用户提供的消息处理函数
			if buffer = append(buffer, msg); len(buffer) == cap(buffer) {
				if err := handler(); err != nil {
					return err
				}
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
