package config

import (
	"github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"
	"os"
)

type Loader struct {
	configPaths []string
}

func NewLoader(opts ...LoaderOption) *Loader {
	l := &Loader{}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

type LoaderOption func(l *Loader)

func WithConfigPaths(configPath ...string) LoaderOption {
	return func(l *Loader) {
		l.configPaths = configPath
	}
}

func (l *Loader) LoadConfig() (config.Config, error) {
	configPath, err := l.getConfigPath()
	if err != nil {
		return nil, err
	}
	c := config.New(
		config.WithSource(
			file.NewSource(configPath),
		),
	)
	//1.加载配置文件或配置中心中的内容。
	//2.将配置内容解析为可使用的结构。
	if err := c.Load(); err != nil {
		return nil, err
	}
	return c, nil
}

// getConfigPath 将存在的文件路径响应,如果所有路径都找不到文件则响应Err
func (l *Loader) getConfigPath() (string, error) {
	for _, configPath := range l.configPaths {
		if _, err := os.Stat(configPath); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return "", err
		}
		return configPath, nil
	}
	return "", os.ErrNotExist
}
