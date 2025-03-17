package etcd

import (
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/go-kratos/kratos/v2/registry"
)

func TestRegistry(t *testing.T) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://192.168.3.13:20000", "http://192.168.3.13:20002", "http://192.168.3.13:20004"},
		DialTimeout: time.Second, DialOptions: []grpc.DialOption{grpc.WithBlock()},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	//注册服务实例
	s := &registry.ServiceInstance{
		ID:   "0",
		Name: "helloworld",
	}

	r := New(client)               //构造一个etcd client包装器
	w, err := r.Watch(ctx, s.Name) //监听这个服务名称
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = w.Stop()
	}()
	//当监听的服务实例发生变更时,打印日志
	go func() {
		for {
			res, err1 := w.Next()
			if err1 != nil {
				return
			}
			t.Logf("watch: %d", len(res))
			for _, r := range res {
				t.Logf("next: %+v", r)
			}
		}
	}()
	time.Sleep(time.Second)

	//注册服务
	if err1 := r.Register(ctx, s); err1 != nil {
		t.Fatal(err1)
	}
	time.Sleep(time.Second)

	//获取服务实例列表
	res, err := r.GetService(ctx, s.Name)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 && res[0].Name != s.Name {
		t.Errorf("not expected: %+v", res)
	}

	//取消注册
	if err1 := r.Deregister(ctx, s); err1 != nil {
		t.Fatal(err1)
	}
	time.Sleep(time.Second)

	res, err = r.GetService(ctx, s.Name)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 0 {
		t.Errorf("not expected empty")
	}
	select {}
}

func TestRegistryA(t *testing.T) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://192.168.3.13:20000", "http://192.168.3.13:20002", "http://192.168.3.13:20004"},
		DialTimeout: time.Second, DialOptions: []grpc.DialOption{grpc.WithBlock()},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	//注册服务实例
	s := &registry.ServiceInstance{
		ID:   "0",
		Name: "ts-fund",
	}

	r := New(client)               //构造一个etcd client包装器
	w, err := r.Watch(ctx, s.Name) //监听这个服务名称
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = w.Stop()
	}()
	//当监听的服务实例发生变更时,打印日志
	go func() {
		for {
			res, err1 := w.Next()
			if err1 != nil {
				return
			}
			t.Logf("watch: %d", len(res))
			for _, r := range res {
				t.Logf("next: %+v", r)
			}
		}
	}()
	time.Sleep(time.Second)

	//注册服务
	if err1 := r.Register(ctx, s); err1 != nil {
		t.Fatal(err1)
	}
	time.Sleep(time.Second)

	//获取服务实例列表
	res, err := r.GetService(ctx, s.Name)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range res {
		t.Logf("next: %+v", r)
	}
	select {}
}

func TestRegistryB(t *testing.T) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://192.168.3.13:20000", "http://192.168.3.13:20002", "http://192.168.3.13:20004"},
		DialTimeout: time.Second, DialOptions: []grpc.DialOption{grpc.WithBlock()},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	//注册服务实例
	s := &registry.ServiceInstance{
		ID:   "1",
		Name: "ts-fund",
	}

	r := New(client)               //构造一个etcd client包装器
	w, err := r.Watch(ctx, s.Name) //监听这个服务名称
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = w.Stop()
	}()
	//当监听的服务实例发生变更时,打印日志
	go func() {
		for {
			res, err1 := w.Next()
			if err1 != nil {
				return
			}
			t.Logf("watch: %d", len(res))
			for _, r := range res {
				t.Logf("next: %+v", r)
			}
		}
	}()
	time.Sleep(time.Second)

	//注册服务
	if err1 := r.Register(ctx, s); err1 != nil {
		t.Fatal(err1)
	}
	time.Sleep(time.Second)

	//获取服务实例列表
	res, err := r.GetService(ctx, s.Name)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range res {
		t.Logf("next: %+v", r)
	}
	select {}
}

func TestHeartBeat(t *testing.T) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: time.Second, DialOptions: []grpc.DialOption{grpc.WithBlock()},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	s := &registry.ServiceInstance{
		ID:   "0",
		Name: "helloworld",
	}

	go func() {
		r := New(client)
		w, err1 := r.Watch(ctx, s.Name)
		if err1 != nil {
			return
		}
		defer func() {
			_ = w.Stop()
		}()
		for {
			res, err2 := w.Next()
			if err2 != nil {
				return
			}
			t.Logf("watch: %d", len(res))
			for _, r := range res {
				t.Logf("next: %+v", r)
			}
		}
	}()
	time.Sleep(time.Second)

	// new a server
	r := New(client,
		RegisterTTL(2*time.Second),
		MaxRetry(5),
	)

	key := fmt.Sprintf("%s/%s/%s", r.opts.namespace, s.Name, s.ID)
	value, _ := marshal(s)
	r.lease = clientv3.NewLease(r.client)
	leaseID, err := r.registerWithKV(ctx, key, value)
	if err != nil {
		t.Fatal(err)
	}

	// wait for lease expired
	time.Sleep(3 * time.Second)

	res, err := r.GetService(ctx, s.Name)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 0 {
		t.Errorf("not expected empty")
	}

	go r.heartBeat(ctx, leaseID, key, value)

	time.Sleep(time.Second)
	res, err = r.GetService(ctx, s.Name)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) == 0 {
		t.Errorf("reconnect failed")
	}
}
