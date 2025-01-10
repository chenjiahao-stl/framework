package config

import (
	"github.com/chenjiahao-stl/framework/conf"
	"github.com/go-kratos/kratos/v2/config"
)

// LoadConfig 加载nacos上的配置 或者 加载环境变量的配置到指定的对象中 或者 服务本地配置文件加载/*
func LoadConfig(bootstrap *conf.Bootstrap, configPath []string, appConfig ...interface{}) error {
	loader := NewLoader(WithConfigPaths(configPath...))

	c, err := loader.LoadConfig()
	if err != nil {
		return err
	}
	//将配置内容绑定到指定的结构体中
	if err := c.Scan(bootstrap); err != nil {
		return err
	}

	//TODO 先将组件配置加载到bootstrap中,其中包含nacos的相关信息,再将nacos配置中心的数据加载进Config中,
	//TODO 再重新将最新的数据绑定到bootstrap和appConfig中去.

	if err := reloadScanConfigs(c, append(appConfig, bootstrap)...); err != nil {
		return err
	}
	return nil
}

func reloadScanConfigs(c config.Config, configs ...interface{}) error {
	for _, config := range configs {
		if err := c.Scan(config); err != nil {
			return err
		}
	}
	return nil
}
