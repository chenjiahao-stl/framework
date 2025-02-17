package logger

import (
	"fmt"
	"github.com/chenjiahao-stl/framework/conf"
	"testing"
	"time"
)

func TestLogger(t *testing.T) {
	conf.ServerName = "order"
	_, cancel, err := NewLogger(&LogConfig{
		LogDir: "D:\\Data\\logs\\log",
		//LogName:     "ts-order",
		MaxSize:     10,
		MaxBackups:  3,
		MaxAge:      3,
		Compress:    true,
		Development: false,
	}, &conf.Logger{
		OutputType: conf.Logger_OUT_PUT_FILE,
		BizLogPath: "D:\\Data\\logs\\biz",
	}, WithKafkaProduct(func() (BusinessWrite, error) {

		return nil, nil
	}))
	if err != nil {
		return
	}
	defer cancel()
	helper := NewHelper()
	helper.Infof("CreateOrder id:%v", 123456)
	fmt.Println("1111")
	// 阻塞，直到收到完成信号
	select {
	case <-helper.logger.doneCh:
		t.Log("Received done signal, exiting test")
	case <-time.After(5 * time.Second): // 超时处理
		t.Fatal("Test timed out")
	}
}
