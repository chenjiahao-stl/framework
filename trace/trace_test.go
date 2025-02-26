package trace

import (
	"github.com/chenjiahao-stl/framework/conf"
	"testing"
)

func TestA(t *testing.T) {
	InitJaegerTracer(&conf.Data_Tracer{
		Url: "http://127.0.0.1:14268/api/traces",
	}, "addaaa")
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
	select {}

}
