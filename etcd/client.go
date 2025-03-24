package etcd

import (
	"github.com/chenjiahao-stl/framework/conf"
	"github.com/chenjiahao-stl/framework/logger"
	"github.com/chenjiahao-stl/framework/netutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

type EtcdClient struct {
	client *clientv3.Client
	log    *logger.Helper[logger.BusinessStep]
}

func NewEtcdClient(config *conf.Data_Etcd) (*EtcdClient, func(), error) {
	etcdClient, f, err := newEtcdClient(config)
	return &EtcdClient{
		client: etcdClient,
		log:    logger.NewHelper[logger.BusinessStep]("etcd-client"),
	}, f, err
}

func newEtcdClient(config *conf.Data_Etcd) (*clientv3.Client, func(), error) {
	address := netutil.JoinHostPort(config.Host, config.Port)
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   address,
		DialTimeout: 5 * time.Second,
		Username:    config.Username,
		Password:    config.Password,
	})
	if err != nil {
		return nil, nil, err
	}
	return client, func() { client.Close() }, nil
}

func (clit *EtcdClient) Client() *clientv3.Client {
	return clit.client
}
