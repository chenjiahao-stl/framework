package kafka

import (
	"crypto/tls"
	"github.com/IBM/sarama"
)

type ClientConfigOption func(config *sarama.Config)

func WithTLS(t *tls.Config) ClientConfigOption {
	return func(config *sarama.Config) {
		config.Net.TLS.Config = t
		config.Net.TLS.Enable = true
	}
}
