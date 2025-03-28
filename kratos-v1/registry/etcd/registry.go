package etcd

import (
	"context"
	"fmt"
	"github.com/chenjiahao-stl/framework/conf"
	"github.com/chenjiahao-stl/framework/etcd"
	"github.com/chenjiahao-stl/framework/logger"
	"math/rand"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/go-kratos/kratos/v2/registry"
)

var (
	_ registry.Registrar = (*Registry)(nil)
	_ registry.Discovery = (*Registry)(nil)
)

const KEEP_RETRY int = -1

// Option is etcd registry option.
type Option func(o *options)

type options struct {
	ctx       context.Context
	namespace string
	ttl       time.Duration //租约过期时间
	maxRetry  int           //心跳检测机制, 服务最大重试 注册次数
}

// Context with registry context.
func Context(ctx context.Context) Option {
	return func(o *options) { o.ctx = ctx }
}

// Namespace with registry namespace.
func Namespace(ns string) Option {
	return func(o *options) { o.namespace = ns }
}

// RegisterTTL with register ttl.
func RegisterTTL(ttl time.Duration) Option {
	return func(o *options) { o.ttl = ttl }
}

func MaxRetry(num int) Option {
	return func(o *options) { o.maxRetry = num }
}

func KeepRetry() Option {
	return func(o *options) {
		o.maxRetry = KEEP_RETRY
	}
}

func NameSpace(namespace string) Option {
	return func(o *options) {
		o.namespace = namespace
	}
}

// Registry is etcd registry.
type Registry struct {
	opts   *options
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
	/*
		ctxMap is used to store the context cancel function of each service instance.
		When the service instance is deregistered, the corresponding context cancel function is called to stop the heartbeat.
	*/
	ctxMap map[*registry.ServiceInstance]context.CancelFunc
	log    *logger.Helper[logger.BusinessStep]
}

func NewRegister(register *conf.Registry, cli *etcd.EtcdClient) (*Registry, error) {
	if register == nil || register.Etcd == nil {
		return nil, fmt.Errorf("etcd Registry config is nil")
	}
	opts := []Option{}
	if register.Namespace != "" {
		opts = append(opts, NameSpace(fmt.Sprintf("/microservices/%s", register.GetNamespace())))
	}
	if register.Etcd.MaxRetry < 0 {
		register.Etcd.MaxRetry = -1
		opts = append(opts, KeepRetry())
	} else if register.Etcd.MaxRetry > 0 {
		opts = append(opts, MaxRetry(int(register.Etcd.MaxRetry)))
	}
	return New(cli.Client(), opts...), nil
}

// New creates etcd registry
func New(client *clientv3.Client, opts ...Option) (r *Registry) {
	op := &options{
		ctx:       context.Background(),
		namespace: "/microservices",
		ttl:       time.Second * 15,
	}
	for _, o := range opts {
		o(op)
	}
	return &Registry{
		opts:   op,
		client: client,
		kv:     clientv3.NewKV(client),
		ctxMap: make(map[*registry.ServiceInstance]context.CancelFunc),
		log:    logger.NewHelper[logger.BusinessStep]("etcd-register"),
	}
}

// Register the registration.
func (r *Registry) Register(ctx context.Context, service *registry.ServiceInstance) error {
	key := fmt.Sprintf("%s/%s/%s", r.opts.namespace, service.Name, service.ID) // /microservices/fund/0
	value, err := marshal(service)                                             //序列化成json字符串
	if err != nil {
		return err
	}
	if r.lease != nil {
		r.lease.Close()
	}
	r.lease = clientv3.NewLease(r.client) //创建一个租约对象
	leaseID, err := r.registerWithKV(ctx, key, value)
	if err != nil {
		return err
	}

	hctx, cancel := context.WithCancel(r.opts.ctx)
	r.ctxMap[service] = cancel
	go r.heartBeat(hctx, leaseID, key, value) //启动异步心跳机制
	return nil
}

// Deregister the registration.
func (r *Registry) Deregister(ctx context.Context, service *registry.ServiceInstance) error {
	defer func() {
		if r.lease != nil {
			r.lease.Close()
		}
	}()
	// cancel heartbeat
	if cancel, ok := r.ctxMap[service]; ok {
		cancel()
		delete(r.ctxMap, service)
	}
	key := fmt.Sprintf("%s/%s/%s", r.opts.namespace, service.Name, service.ID)
	_, err := r.client.Delete(ctx, key)
	return err
}

// GetService return the service instances in memory according to the service name.
func (r *Registry) GetService(ctx context.Context, name string) ([]*registry.ServiceInstance, error) {
	key := fmt.Sprintf("%s/%s", r.opts.namespace, name)
	resp, err := r.kv.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	items := make([]*registry.ServiceInstance, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		si, err := unmarshal(kv.Value)
		if err != nil {
			return nil, err
		}
		if si.Name != name {
			continue
		}
		items = append(items, si)
	}
	return items, nil
}

// Watch creates a watcher according to the service name.
func (r *Registry) Watch(ctx context.Context, name string) (registry.Watcher, error) {
	key := fmt.Sprintf("%s/%s", r.opts.namespace, name)
	return newWatcher(ctx, key, name, r.client)
}

// registerWithKV create a new lease, return current leaseID
func (r *Registry) registerWithKV(ctx context.Context, key string, value string) (clientv3.LeaseID, error) {
	grant, err := r.lease.Grant(ctx, int64(r.opts.ttl.Seconds())) //申请一个ttl秒的租约
	if err != nil {
		return 0, err
	}
	_, err = r.client.Put(ctx, key, value, clientv3.WithLease(grant.ID)) //Put 一个KV键值对,让它与租约关联起来,从而实现ttl秒后自动过期
	if err != nil {
		return 0, err
	}
	return grant.ID, nil
}

func (r *Registry) heartBeat(ctx context.Context, leaseID clientv3.LeaseID, key string, value string) {
	curLeaseID := leaseID
	kac, err := r.client.KeepAlive(ctx, leaseID) //开启自动永久续租
	if err != nil {
		curLeaseID = 0
	}
	randSource := rand.New(rand.NewSource(time.Now().Unix()))

	for {
		//租约失效就把curLeaseID设置为0
		if curLeaseID == 0 {
			// try to registerWithKV
			var retreat []int
			//maxRetry: 租约失效重试次数
			for retryCnt := 0; retryCnt < r.opts.maxRetry; retryCnt++ {
				if ctx.Err() != nil {
					return
				}
				// prevent infinite blocking
				idChan := make(chan clientv3.LeaseID, 1)
				errChan := make(chan error, 1)
				cancelCtx, cancel := context.WithCancel(ctx)
				go func() {
					defer cancel()
					id, registerErr := r.registerWithKV(cancelCtx, key, value) //响应id为租约id
					if registerErr != nil {
						errChan <- registerErr
					} else {
						idChan <- id
					}
				}()

				select {
				case <-time.After(3 * time.Second):
					cancel()
					continue
				case <-errChan:
					continue
				case curLeaseID = <-idChan:
				}

				kac, err = r.client.KeepAlive(ctx, curLeaseID) //开启自动永久续租
				if err == nil {
					break
				}
				retreat = append(retreat, 1<<retryCnt)
				time.Sleep(time.Duration(retreat[randSource.Intn(len(retreat))]) * time.Second)
			}
			if _, ok := <-kac; !ok {
				// retry failed
				return
			}
		}

		select {
		//每秒会续约一次,所以就会收到一次应答
		case _, ok := <-kac:
			if !ok {
				if ctx.Err() != nil {
					// channel closed due to context cancel
					return
				}
				// need to retry registration
				curLeaseID = 0 //租约失效就把curLeaseID设置为0
				continue
			}
		case <-r.opts.ctx.Done():
			return
		}
	}
}
