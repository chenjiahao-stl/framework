package trace

import (
	"context"
	"github.com/chenjiahao-stl/framework/conf"
	"github.com/chenjiahao-stl/framework/logger"
	"github.com/chenjiahao-stl/framework/logger/business"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"log"
	"net/http"
	"testing"
	"time"
)

func TestA(t *testing.T) {

	// 初始化 Jaeger 和 OpenTelemetry
	tp, _, err := InitJaegerTracer(&conf.Data_Tracer{
		Url: "http://192.168.3.13:14268/api/traces",
	}, "addacc")
	if err != nil {
		log.Fatalf("Failed to initialize tracer: %v", err)
	}
	defer ShutdownTracer(tp)

	// 创建一个 HTTP 服务器
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// 获取全局追踪器
		tracer := otel.Tracer("example.com/trace")
		//ctx, span := tracer.Start(r.Context(), "incoming_request")
		ctx, span := tracer.Start(r.Context(), "incoming_request")
		defer span.End()

		// 模拟一些工作
		time.Sleep(100 * time.Millisecond)

		// 在 span 中记录事件
		span.AddEvent("Request received", trace.WithAttributes())

		// 返回响应
		w.Write([]byte("Hello, World!"))

		// 模拟一个子请求
		//subTracer := otel.Tracer("example.com/trace")
		_, subSpan := tracer.Start(ctx, "sub-request")
		defer subSpan.End()
		time.Sleep(50 * time.Millisecond)
	})

	// 启动 HTTP 服务
	log.Println("Starting server on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}

	select {}

}

type IncrenmentAmountRequest struct {
	Appid           string `json:"appid"`
	AccountId       string `json:"account_id"`
	ExOrderId       string `json:"ex_order_id"`
	TransactionCode string `json:"transaction_code"`
}

func TestB(t *testing.T) {
	// 初始化 Jaeger 和 OpenTelemetry
	tp, _, err := InitJaegerTracer(&conf.Data_Tracer{
		Url: "http://192.168.3.13:14268/api/traces",
	}, "addbbb")
	if err != nil {
		log.Fatalf("Failed to initialize tracer: %v", err)
	}
	defer ShutdownTracer(tp)

	conf.ServerName = "ts-order"
	_, cancel, err := logger.NewLogger(&logger.LogConfig{
		LogDir: "D:\\Data\\logs\\log",
		//LogName:     "ts-order",
		MaxSize:     10,
		MaxBackups:  3,
		MaxAge:      3,
		Compress:    true,
		Development: false,
	}, &conf.Logger{
		OutputType: conf.Logger_OUT_PUT_KAFKA,
		BizLogPath: "D:\\Data\\logs\\biz",
	}, logger.WithKafkaProduct(business.NewKafkaSend(&conf.Data_Kafka{
		Addrs: append([]string{"192.168.3.13:9192", "192.168.3.13:9292", "192.168.3.13:9392"}),
	}, "topic-order")))
	if err != nil {
		return
	}
	defer cancel()
	helper := logger.NewHelper[logger.BusinessStep]("test-trace")

	// 模拟一些工作
	time.Sleep(500 * time.Millisecond)

	go func() {
		tracer := otel.Tracer("example.com/trace")
		ctx, span := tracer.Start(context.TODO(), "incoming_request")
		defer span.End()

		// 模拟一些工作
		time.Sleep(100 * time.Millisecond)

		// 在 span 中记录事件
		span.AddEvent("Request received", trace.WithAttributes(attribute.String("sss", "qqq")))

		_, subSpan := tracer.Start(ctx, "sub-request")
		subSpan.AddEvent("SubSpan Event", trace.WithAttributes(attribute.String("key111", "value11")))
		defer subSpan.End()
	}()

	go func() {
		req := &IncrenmentAmountRequest{
			Appid:           "a264644s",
			AccountId:       "A456L",
			ExOrderId:       "D-523145",
			TransactionCode: "DEPOSIT",
		}
		//marshal, _ := json.Marshal(req)
		ctx, span := helper.Start(context.TODO(), "FreezeAmount")
		defer span.End()
		helper.InfoWithBusiness(ctx, logger.BusinessStep{
			Step: "Fundorder Request",
			Msg:  req,
		})
		ctx2, span2 := helper.Start(ctx, "freezeAmount")
		defer span2.End()
		helper.InfoWithContext(ctx2, "FreezeAmount core_order_id: 1234567")
	}()

	time.Sleep(100 * time.Millisecond)
	select {}
}
