package test

import (
	"context"
	"fmt"
	"github.com/chenjiahao-stl/framework/conf"
	"github.com/chenjiahao-stl/framework/logger"
	"github.com/chenjiahao-stl/framework/logger/business"
	"testing"
)

func TestLogger(t *testing.T) {
	//ch := make(chan struct{})
	conf.ServerName = "order"
	_, cancel, err := logger.NewLogger(&conf.Logger{
		OutputType: conf.Logger_OUT_PUT_KAFKA,
		BizLogPath: "D:\\Data\\logs\\biz",
		LogConfig: &conf.Logger_LogConfig{
			LogDir: "D:\\Data\\logs\\log",
			//LogName:     "ts-order",
			MaxSize:     10,
			MaxBackups:  3,
			MaxAge:      3,
			Compress:    true,
			Development: false,
		},
	}, logger.WithKafkaProduct(business.NewKafkaSend(&conf.Data_Kafka{
		Addrs: append([]string{"192.168.3.13:9192", "192.168.3.13:9292", "192.168.3.13:9392"}),
	}, "topic111")))
	if err != nil {
		return
	}
	defer cancel()
	helper := logger.NewHelper[logger.BusinessStep]("test")
	helper.Infof("CreateOrder id:%v", 123456)
	helper.InfoWithBusiness(context.Background(), logger.BusinessStep{
		Step: "Create Fundorder",
		Msg:  "fund_order_id: 123456",
	})
	fmt.Println("1111")
	select {}
}
