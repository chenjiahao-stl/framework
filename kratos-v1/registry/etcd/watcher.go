package etcd

import (
	"context"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/go-kratos/kratos/v2/registry"
)

var _ registry.Watcher = (*watcher)(nil)

type watcher struct {
	key         string
	ctx         context.Context
	cancel      context.CancelFunc
	client      *clientv3.Client
	watchChan   clientv3.WatchChan
	watcher     clientv3.Watcher
	kv          clientv3.KV
	first       bool
	serviceName string
}

func newWatcher(ctx context.Context, key, name string, client *clientv3.Client) (*watcher, error) {
	w := &watcher{
		key:         key,
		client:      client,
		watcher:     clientv3.NewWatcher(client),
		kv:          clientv3.NewKV(client),
		first:       true,
		serviceName: name,
	}
	w.ctx, w.cancel = context.WithCancel(ctx)
	w.watchChan = w.watcher.Watch(w.ctx, key, clientv3.WithPrefix(), clientv3.WithRev(0), clientv3.WithKeysOnly())
	//主动请求 etcd 发送当前 revision 进度，避免 Watch() 监听长时间无响应
	//什么时候使用 RequestProgress()
	//低频变更场景：如果监听的 key 很少变化，防止 Watch() 长时间无响应。
	//连接健康检查：如果 Watch() 监听的 key 没有变更，定期请求进度，避免 etcd 连接假死。
	//防止 Watch() 断开重连：可以减少不必要的 Watch() 重新建立连接的次数。
	err := w.watcher.RequestProgress(w.ctx)
	if err != nil {
		return nil, err
	}
	return w, nil
}

// Next  Next() 主要用于 服务发现（Service Discovery），一般配合 etcd 作为注册中心使用，比如：
// 负载均衡：获取最新的服务列表，分配请求。
// 健康检查：如果某个服务实例失效，可以通过 Next() 监听并移除它。
// 动态扩容：当新服务实例加入时，Next() 会立即返回更新后的实例列表。
// Next() 的行为
// 当新的服务实例加入 etcd 时，Next() 返回更新后的服务实例列表。
// 当某个实例被移除时，Next() 返回一个新的列表，不再包含该实例。
// 如果 etcd 断开，Next() 可能会返回 error，调用者需要重新建立 Watch。
// 默认是阻塞的，直到检测到变化才返回。 /**
func (w *watcher) Next() ([]*registry.ServiceInstance, error) {
	if w.first {
		item, err := w.getInstance()
		w.first = false
		return item, err
	}

	select {
	case <-w.ctx.Done():
		return nil, w.ctx.Err()
	case watchResp, ok := <-w.watchChan:
		if !ok || watchResp.Err() != nil {
			time.Sleep(time.Second)
			err := w.reWatch() //watch 监听重连
			if err != nil {
				return nil, err
			}
		}
		return w.getInstance() //获取当前etcd最新的服务实例列表
	}
}

func (w *watcher) Stop() error {
	w.cancel()
	return w.watcher.Close()
}

func (w *watcher) getInstance() ([]*registry.ServiceInstance, error) {
	resp, err := w.kv.Get(w.ctx, w.key, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	items := make([]*registry.ServiceInstance, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		si, err := unmarshal(kv.Value)
		if err != nil {
			return nil, err
		}
		if si.Name != w.serviceName {
			continue
		}
		items = append(items, si)
	}
	return items, nil
}

func (w *watcher) reWatch() error {
	w.watcher.Close()
	w.watcher = clientv3.NewWatcher(w.client)
	w.watchChan = w.watcher.Watch(w.ctx, w.key, clientv3.WithPrefix(), clientv3.WithRev(0), clientv3.WithKeysOnly())
	return w.watcher.RequestProgress(w.ctx)
}
