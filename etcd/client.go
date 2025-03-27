package etcd

import (
	"fmt"
	"github.com/chenjiahao-stl/framework/conf"
	"github.com/chenjiahao-stl/framework/logger"
	"github.com/chenjiahao-stl/framework/netutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"strings"
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
	var address []string
	if config.GetAddress() != "" {
		address = strings.Split(config.GetAddress(), ",")
	} else if config.GetHost() != "" && config.GetPort() != 0 {
		address = netutil.JoinHostPort(config.Host, config.Port)
	} else {
		return nil, nil, fmt.Errorf("etcd config is nil")
	}
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
