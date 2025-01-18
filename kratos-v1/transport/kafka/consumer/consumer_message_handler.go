package kafka

import (
	"context"
	"github.com/IBM/sarama"
)

type MysqlMessageHandler struct {
}

func (h MysqlMessageHandler) IsRepeat(ctx context.Context) (bool, error) {

	return false, nil
}
func (h MysqlMessageHandler) SetError(ctx context.Context, message *sarama.ConsumerMessage) error {

	return nil
}

type RedisMessageHandler struct {
}

func (h RedisMessageHandler) IsRepeat(ctx context.Context) (bool, error) {
	return false, nil
}
func (h RedisMessageHandler) SetError(ctx context.Context, message *sarama.ConsumerMessage) error {

	return nil
}
