package kafka

import (
	"context"
	"github.com/IBM/sarama"
	"github.com/go-kratos/kratos/v2/log"
	"golang.org/x/sync/errgroup"
	"sync"
	"time"
)

type ServerOption func(*KafkaServer)

func WithAddress(addrs []string) ServerOption {
	return func(server *KafkaServer) {
		server.addrs = addrs
	}
}

func WithUserName(userName string) ServerOption {
	return func(server *KafkaServer) {
		server.userName = userName
	}
}

func WithPassword(password string) ServerOption {
	return func(server *KafkaServer) {
		server.password = password
	}
}

func WithVersion(version string) ServerOption {
	return func(server *KafkaServer) {
		server.version = version
	}
}

// ConsumerConfig 消费者配置
type ConsumerConfig struct {
	Brokers       []string                                             // Kafka broker 地址列表
	ConsumerGroup string                                               // 消费者组名称
	Topics        []string                                             // 订阅的主题
	Version       string                                               // Kafka 版本
	HandlerFunc   func(context.Context, *sarama.ConsumerMessage) error // 消息处理函数
}

type HandlerFunc func(context.Context, *sarama.ConsumerMessage) error

type ConsumerRouter struct {
	topic          string
	groupId        string
	batchSize      int
	asyncWorkerCap int
	async          bool
	client         sarama.ConsumerGroup
	handler        HandlerFunc // 消息处理函数
}

func (r *ConsumerRouter) SetTopic(topic string) *ConsumerRouter {
	r.topic = topic
	return r
}

func (r *ConsumerRouter) SetGroupId(groupId string) *ConsumerRouter {
	r.groupId = groupId
	return r
}

func (r *ConsumerRouter) SetBatchSize(batchSize int) *ConsumerRouter {
	r.batchSize = batchSize
	return r
}

func (r *ConsumerRouter) SetAsyncWorkerCap(asyncWorkerCap int) *ConsumerRouter {
	r.async = true
	if asyncWorkerCap <= 10 {
		asyncWorkerCap = 10
	}
	r.asyncWorkerCap = asyncWorkerCap
	return r
}

// KafkaConsumer Kafka 消费者工具类
type KafkaServer struct {
	addrs          []string          // Kafka broker 地址列表
	userName       string            // Kafka 账号名
	password       string            // Kafka 密码
	version        string            // Kafka 版本号
	autoCommit     bool              //是否自动提交
	routers        []*ConsumerRouter //消费者路由列表
	consumerConfig ConsumerConfig
	config         *sarama.Config
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
}

// NewKafkaServer 创建一个 Kafka 消费者
func NewKafkaServer(opts ...ServerOption) (*KafkaServer, error) {
	/**
	如何设计一个kafka消费者？
	1.支持单条消息消费
	2.支持批量消息消费
	3.封装消费者通道，支持创建多条通道，可配置消费者组id
	4.是否自动提交
	5.消息失败策略、失败重试策略
	6.支持扩展middlewares
	*/
	server := &KafkaServer{
		wg:     sync.WaitGroup{},
		ctx:    context.Background(),
		cancel: context.CancelFunc(func() {}),
	}
	//给server赋值
	for _, opt := range opts {
		opt(server)
	}

	version, err := sarama.ParseKafkaVersion(server.version)
	if err != nil {
		return nil, err
	}

	// Sarama 配置
	config := sarama.NewConfig()
	config.Version = version

	/**
	用于存储从 Kafka 中拉取到的消息的 内存缓冲区大小。
	该配置定义了消息队列的大小，消费者从 Kafka 拉取消息后，这些消息会被暂存到一个内存缓冲区中，直到消费者处理它们。
	如果消费者处理消息的速度跟不上拉取消息的速度，缓冲区就会满。如果缓冲区满了，消费者会被阻塞，直到有足够的空间来存放新的消息。
	默认值为 1000，表示缓冲区可以容纳最多 1000 条消息。
	*/
	config.ChannelBufferSize = 10000

	config.Net.SASL.User = server.userName
	config.Net.SASL.Password = server.password

	/**
	设置 Kafka 客户端刷新元数据的频率,Kafka 客户端会定期从 Kafka 集群获取新的元数据（如 broker 列表和分区信息）。
	*/
	config.Metadata.RefreshFrequency = 10 * time.Minute
	/**
	NewBalanceStrategySticky 是一种基于 粘性（sticky）的负载均衡策略。它的目标是让消费者尽可能保持自己之前分配的分区，即使在消费者数量变化的情况下，
	也尽量避免重新分配消费者的分区，以减少消费者的中断。
	*/
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()}
	/**
	设置每次从 Kafka 消费的默认字节数。
	这个值控制每次从 Kafka 服务器拉取消息的最大字节数。默认值通常为 1MB。
	*/
	config.Consumer.Fetch.Default = 1024 * 1024
	//用于控制消费者从 Kafka 集群拉取消息时最大等待的时间。它定义了消费者在没有收到消息时最多等待多久，再进行下一次拉取请求。
	config.Consumer.MaxWaitTime = 100 * time.Millisecond
	//它用于设置消费者在处理每条消息时的 最大处理时间。如果消费者在该时间内未处理完消息，消息将被认为是处理失败，可能会导致消息重新消费。
	config.Consumer.MaxProcessingTime = 10 * time.Second
	//用于控制消费者是否返回处理错误。如果设置为 false，消费者在处理消息时遇到的错误将不会被返回到 Errors 通道，
	//而是会被丢弃或忽略；如果设置为 true，错误将被发送到 Errors 通道，供应用程序处理。
	config.Consumer.Return.Errors = false

	//用于设置消费者偏移量（offset）在 Kafka 中保留的时间长度。这个参数控制了消费者在 Kafka 中的消费偏移量数据保留多久。
	//Kafka 会根据这个保留时间来决定是否删除已经不再需要的偏移量信息。
	config.Consumer.Offsets.Retention = 24 * time.Hour
	config.Consumer.Offsets.Initial = sarama.OffsetNewest         // 默认从最新的消息开始消费
	config.Consumer.Offsets.AutoCommit.Enable = true              //用于控制消费者在消费完消息后是否自动提交消息的偏移量。
	config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second //用于设置自动提交偏移量的时间间隔。

	/**
	用于设置消费者组的会话超时时间，即在消费者组中，消费者与 Kafka 协调器（协调消费者组的成员）保持连接的最大时长。
	如果消费者在指定的超时时间内没有响应（例如没有发送心跳），则 Kafka 会认为该消费者已失联，自动触发消费者重平衡，将消费者组中的分区重新分配给其他活跃的消费者。
	默认值通常为 10 * time.Second，即 10 秒。这个值表示如果消费者在 10 秒内没有发送心跳，Kafka 会认为该消费者失联并开始执行重新平衡。
	*/
	config.Consumer.Group.Session.Timeout = 10 * time.Second
	config.Consumer.Group.Heartbeat.Interval = 3 * time.Second //定义了消费者向 Kafka 发送心跳的间隔时间,即每 3 秒向 Kafka 发送一次心跳。
	server.config = config

	//// 创建消费者组
	//client, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.ConsumerGroup, config)
	//if err != nil {
	//	return nil, err
	//}
	//
	//ctx, cancel := context.WithCancel(context.Background())

	return server, nil
}

func (s *KafkaServer) ConsumerRouter(topic string, handler HandlerFunc) *ConsumerRouter {
	router := &ConsumerRouter{
		topic:   topic,
		handler: handler,
		async:   false,
	}
	s.routers = append(s.routers, router)
	return router
}

//// Start 开始消费
//func (kc *KafkaConsumer) Start() error {
//	kc.wg.Add(1)
//	go func() {
//		defer kc.wg.Done()
//		for {
//			if err := kc.client.Consume(kc.ctx, kc.config.Topics, kc); err != nil {
//				log.Printf("Kafka consume error: %v", err)
//			}
//			// 如果 context 被取消，则退出
//			if kc.ctx.Err() != nil {
//				return
//			}
//		}
//	}()
//	return nil
//}

//// Stop 停止消费
//func (kc *KafkaConsumer) Stop() {
//	kc.cancel()
//	kc.wg.Wait()
//	if err := kc.client.Close(); err != nil {
//		log.Printf("Error closing Kafka consumer: %v", err)
//	}
//}

func (s *KafkaServer) consumerHasGroup(ctx context.Context, r *ConsumerRouter) error {
	// 创建消费者组
	group, err := sarama.NewConsumerGroup(s.addrs, r.groupId, s.config)
	if err != nil {
		return err
	}
	defer func() {
		group.Close()
	}()
	/**
	Kafka 消费者通常是一个持续运行的进程，它会不断地从 Kafka 主题（topic）中拉取消息并进行处理。
	Consume 方法内部会维护一个消息拉取和处理的循环，直到手动停止消费者或者发生错误。
	因此，你需要在一个 for 循环中不断调用 Consume，以确保消费者始终处于活跃状态，能够持续消费消息。
	为什么要使用 for 循环？
	1.保证消费者持续运行：Consume 方法是一个阻塞调用，会一直等待消息的到来。for 循环确保消费者在没有错误时能够不断消费 Kafka 中的消息。
	2.消息拉取的流控：sarama 会按需拉取消息，Consume 方法是一个内部轮询机制，持续进行消息拉取并调用 ConsumeClaim 来处理消息。for 循环保持了这一流程的持续性。
	3.处理多次重启：在某些情况下，消费者可能会因为网络故障、Kafka 服务不可用等原因中断，for 循环的形式使得在发生错误时，消费者可以继续尝试重启并重新连接到 Kafka。
	4.优雅地退出和停止：你可以通过在 for 循环外部控制 ctx，实现优雅地停止消费者组。在程序关闭时，通常会通过 context.Cancel 或超时机制来取消消费者的工作。
	*/
	for {
		log.Infof("[kafka] starting to consumer topic: %s", r.topic)
		if err := s.consumerGroup(ctx, group, r); err != nil {
			log.Errorf("[kafka] failed to consumer topic: %s,err: %v", r.topic, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			//如果发生kafka服务连不上的情况就每隔1s重连一次
			time.Sleep(1 * time.Second)
			continue
		}
	}

}

func (s *KafkaServer) consumerGroup(ctx context.Context, group sarama.ConsumerGroup, r *ConsumerRouter) error {
	//异步批量消息处理
	if r.async {
		return group.Consume(ctx, []string{r.topic}, &MultipleConsumerGroupHandler{})
	}
	//同步单条消息处理
	return group.Consume(ctx, []string{r.topic}, &SingleConsumerGroupHandler{})
}

func (s *KafkaServer) Start(nctx context.Context) error {
	//当父 context 发生异常或取消时，它会影响到所有子 context。但是，反过来，子 context 的异常或取消不会自动传播回父 context。
	s.ctx, s.cancel = context.WithCancel(nctx)
	g, ctx := errgroup.WithContext(s.ctx)

	for _, router := range s.routers {
		g.Go(func() error {
			if router.groupId != "" {
				return s.consumerHasGroup(ctx, router)
			}
			return nil
		})
	}
	return g.Wait()
}

func (s *KafkaServer) Stop(ctx context.Context) error {
	if s.cancel != nil {
		s.cancel()
	}
	return nil
}
