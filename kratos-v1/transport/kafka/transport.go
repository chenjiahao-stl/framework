package kafka

import (
	"github.com/IBM/sarama"
	i_transport "github.com/chenjiahao-stl/framework/kratos-v1/transport"
	"github.com/go-kratos/kratos/v2/transport"
)

// KafkaTransport 实现了 Transporter 接口
// 封装传输协议相关的信息，例如请求头、响应头、请求的操作、传输的端点等。
// 这种设计方式适用于微服务中需要传递传输层上下文信息的场景，可以很方便地扩展以支持更多的传输协议（如 WebSocket、Kafka 等）。
type KafkaTransport struct {
	endpointStr  string
	operationStr string
	reqHeader    Header
	replyHeader  Header
}

// NewKafkaTransport 初始化KafkaTransport,并将kafka的header设置进Transport Header中
func NewKafkaTransport(msg *sarama.ConsumerMessage) *KafkaTransport {
	t := &KafkaTransport{
		endpointStr:  msg.Topic,
		operationStr: msg.Topic,
	}
	for _, header := range msg.Headers {
		t.reqHeader.Set(string(header.Key), string(header.Value))
	}
	return t
}

// Kind 返回传输类型，这里是 Kafka
func (t *KafkaTransport) Kind() transport.Kind {
	return i_transport.Kind_KAFKA
}

// Endpoint 返回kafka topic信息
func (t *KafkaTransport) Endpoint() string {
	return t.endpointStr
}

// Operation 返回Operation
func (t *KafkaTransport) Operation() string {
	return t.operationStr
}

// RequestHeader 返回传输请求头
func (t *KafkaTransport) RequestHeader() transport.Header {
	return t.reqHeader
}

// ReplyHeader 返回传输响应头
func (t *KafkaTransport) ReplyHeader() transport.Header {
	return t.replyHeader
}

// 初始化?
var _ transport.Header = Header{}

type Header map[string][]string

// Get returns the value associated with the passed key.
func (h Header) Get(key string) string {
	if v := h[key]; len(v) > 0 {
		return v[0]
	}
	return ""
}

// Set stores the key-value pair.
func (h Header) Set(key string, value string) {
	h[key] = append(h[key], value)
}

// Add append value to key-values pair.
func (h Header) Add(key string, value string) {
	h[key] = append(h[key], value)
}

// Keys lists the keys stored in this carrier.
func (h Header) Keys() []string {
	keys := make([]string, 0, len(h))
	for k, _ := range h {
		keys = append(keys, k)
	}
	return keys
}

// Values returns a slice of values associated with the passed key.
func (h Header) Values(key string) []string {
	return h[key]
}

func GetHeaderValue(msg *sarama.ConsumerMessage, key string) string {
	for _, header := range msg.Headers {
		if string(header.Key) == key {
			return string(header.Value)
		}
	}
	return ""
}

func DecodeBody(msg *sarama.ConsumerMessage, value any) error {

	return nil
}
