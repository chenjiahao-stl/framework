package trace

import (
	"github.com/chenjiahao-stl/framework/conf"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"log"
	"net/http"
	"testing"
	"time"
)

func TestA(t *testing.T) {
	//InitJaegerTracer(&conf.Data_Tracer{
	//	Url: "http://127.0.0.1:14268/api/traces",
	//}, "addaaa")
	//h := logger.NewHelper(logger.NewLogger("a", "debug"), logger.WithName("aaaa"))
	//
	//ctx, span := h.Start(context.TODO(), "aadddd")
	//
	//h.DebugWithContext(ctx, "aaaaa")
	//ctx, cancel := context.WithCancel(ctx)
	//cancel()
	//ctx = NewSpanContextNotCancel(ctx)
	//h.DebugWithContext(ctx, "aaaaa11111")
	//span.End()
	//fmt.Println(ctx.Err())
	//tracer := otel.Tracer("aaaa")
	//start, span := tracer.Start(context.Background(), "CreateOrder")
	//defer span.End()

	// 初始化 Jaeger 和 OpenTelemetry
	tp, _, err := InitJaegerTracer(&conf.Data_Tracer{
		Url: "http://192.168.3.13:14268/api/traces",
	}, "addaaa")
	if err != nil {
		log.Fatalf("Failed to initialize tracer: %v", err)
	}
	defer ShutdownTracer(tp)

	// 创建一个 HTTP 服务器
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// 获取全局追踪器
		tracer := otel.Tracer("example.com/trace")
		ctx, span := tracer.Start(r.Context(), "incoming_request")
		defer span.End()

		// 模拟一些工作
		time.Sleep(100 * time.Millisecond)

		// 在 span 中记录事件
		span.AddEvent("Request received", trace.WithAttributes())

		// 返回响应
		w.Write([]byte("Hello, World!"))

		// 模拟一个子请求
		subTracer := otel.Tracer("example.com/trace")
		_, subSpan := subTracer.Start(ctx, "sub-request")
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
