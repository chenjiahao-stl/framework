package logger

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/chenjiahao-stl/framework/conf"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/natefinch/lumberjack"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
	"os"
	"sync"
	"time"
)

/*
*Logger
日志组件需要满足几个场景:
1.控制台日志打印
2.将日志发送到kafka异步处理
3.将重要的日志上传到分布式链路平台Jarger/skw等
4.业务流程步骤日志记录, 先写到kafka-> 消费后再写进es...

参考链接：https://www.cnblogs.com/jiujuan/p/17304844.html
*/

var _GL *logger

type Option func(l *logger)

func WithKafkaProduct(newKafkaSendFunc NewKafkaSendFunc) Option {
	return func(l *logger) {
		l.newKafkaSendFunc = newKafkaSendFunc
	}
}

// LogConfig 配置日志结构体
type LogConfig struct {
	LogDir      string // 日志文件保存目录
	LogName     string // 日志文件名称
	MaxSize     int    // 单个日志文件最大尺寸（MB）
	MaxBackups  int    // 保留的最大备份日志数
	MaxAge      int    // 日志最大保存天数
	Compress    bool   // 是否启用压缩
	Development bool   // 是否为开发模式
}

type BusinessWrite interface {
	Write(p []byte) (n int, err error)
	Close() error
}

type NewKafkaSendFunc func() (BusinessWrite, error)

type FileLog struct {
	level  zapcore.Level
	values []interface{}
}

type logger struct {
	log               *zap.Logger
	fileLogCh         chan FileLog
	businessFileLogCh chan []byte
	businessFileZap   BusinessWrite
	newKafkaSendFunc  NewKafkaSendFunc
	doneCh            chan struct{}
	trace             trace.Tracer
}

// NewLogger 创建并初始化 zap logger，结合 lumberjack 进行日志切割
func NewLogger(config *LogConfig, lconf *conf.Logger, opts ...Option) (*logger, func(), error) {
	if lconf.ConsoleLevel == "" {
		lconf.ConsoleLevel = "INFO"
	}
	if lconf.FileLevel == "" {
		lconf.FileLevel = "INFO"
	}
	if lconf.TraceLevel == "" {
		lconf.TraceLevel = "INFO"
	}
	bufferCount := lconf.BufferCount
	if bufferCount <= 0 {
		bufferCount = 100000
	}
	consoleLevel := log.ParseLevel(lconf.ConsoleLevel)
	fileLevel := log.ParseLevel(lconf.FileLevel)
	zapLogger, err := initZapLogger(config, zapcore.Level(consoleLevel), zapcore.Level(fileLevel))
	if err != nil {
		return nil, nil, err
	}
	//var cfg zap.Config
	//logger := zap.Must(cfg.Build())
	l := &logger{
		log:               zapLogger,
		fileLogCh:         make(chan FileLog, bufferCount),
		businessFileLogCh: make(chan []byte, bufferCount),
	}

	for _, opt := range opts {
		opt(l)
	}

	l.trace = otel.Tracer(conf.ServerName)

	_GL = l
	if lconf.OutputType == conf.Logger_OUT_PUT_KAFKA {
		if l.newKafkaSendFunc == nil {
			return nil, nil, fmt.Errorf("newKafkaSendFunc is nil")
		}
		sendFunc, err := l.newKafkaSendFunc()
		if err != nil {
			return nil, nil, err
		}
		l.businessFileZap = sendFunc
	} else {
		bizLogPath := "/val/logs/biz"
		if lconf.BizLogPath != "" {
			bizLogPath = lconf.BizLogPath
		}
		l.businessFileZap = initLumberjack(config, bizLogPath)
	}
	go l.writeLog()

	// 确保日志被正确刷新
	return l, func() {
		Sync(l.log)
		l.businessFileZap.Close()
	}, nil
}

func initLumberjack(config *LogConfig, bizLogPath string) *lumberjack.Logger {
	//是否需要校验文件是否存在?

	// 定义日志输出的配置
	writer := &lumberjack.Logger{
		Filename:   fmt.Sprintf("%s/%s.log", bizLogPath, conf.ServerName),
		MaxSize:    config.MaxSize,    // 每个日志文件最大尺寸,megabytes，M 为单位
		MaxBackups: config.MaxBackups, // 保留旧文件最大份数
		MaxAge:     config.MaxAge,     // days  旧文件最大保存天数
		Compress:   config.Compress,   // 是否压缩日志归档，默认不压缩
	}
	return writer
}

func initZapLogger(config *LogConfig, consoleLevel, fileLevel zapcore.Level) (*zap.Logger, error) {
	writer := initLumberjack(config, config.LogDir)
	/**
	zapcore.NewCore 需要三个配置——Encoder，WriteSyncer，LogLevel。
	1.Encoder:编码器(如何写入日志)。我们将使用开箱即用的NewJSONEncoder()，并使用预先设置的ProductionEncoderConfig()。
	2.WriterSyncer ：指定日志将写到哪里去。我们使用zapcore.AddSync()函数并且将打开的文件句柄传进去。
	3.Log Level：哪种级别的日志将被写入。
	*/
	// 设置日志的输出目标为文件和控制台
	var core zapcore.Core
	if config.Development {
		// 开发环境下，使用更友好的日志输出格式
		core = zapcore.NewCore(
			zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig()), // 控制台输出
			zapcore.NewMultiWriteSyncer(zapcore.AddSync(writer), zapcore.AddSync(os.Stdout)),
			zap.NewAtomicLevelAt(consoleLevel), // 开发环境显示 Debug 级别及以上日志
		)
	} else {
		// 生产环境下，使用 JSON 格式输出
		encoderConfig := zap.NewProductionEncoderConfig()
		encoderConfig.EncodeTime = func(t time.Time, encoder zapcore.PrimitiveArrayEncoder) {
			encoder.AppendString(t.Format(time.RFC3339Nano))
		}
		zapcore.NewTee()
		core = zapcore.NewCore(
			zapcore.NewJSONEncoder(encoderConfig), // JSON 格式
			zapcore.NewMultiWriteSyncer(zapcore.AddSync(writer), zapcore.AddSync(os.Stdout)),
			zap.NewAtomicLevelAt(fileLevel), // 生产环境只输出 Info 级别及以上日志
		)
	}

	// 创建 zap logger
	// zap.AddCaller 输出文件名和行号
	// zap.AddStacktrace 输出调用堆栈
	zapL := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.FatalLevel))
	return zapL, nil
}

func (l *logger) writeLog() {
	eg := errgroup.Group{}
	eg.Go(func() error {
		for item := range l.fileLogCh {
			l.log.Log(item.level, "msg", getLogField(item.values...)...)
		}
		return nil
	})
	eg.Go(func() error {
		for item := range l.businessFileLogCh {
			l.businessFileZap.Write(item)
		}
		return nil
	})
	eg.Wait()
	l.doneCh <- struct{}{}
}

func (l *logger) filelog(level log.Level, keyvals ...interface{}) {
	select {
	case l.fileLogCh <- FileLog{
		level:  zapcore.Level(level),
		values: keyvals,
	}:
	default:
	}
}

func (l *logger) business(val []byte) {
	l.businessFileLogCh <- val
}

// Sync 刷新日志
func Sync(logger *zap.Logger) {
	if err := logger.Sync(); err != nil {
		fmt.Printf("Error syncing logger: %v", err)
	}
}

type Helper[businessData BusinessData] struct {
	logger *logger
}

func NewHelper[bus BusinessData]() *Helper[bus] {
	if _GL == nil {
		panic("logger no init")
	}
	return &Helper[bus]{
		logger: _GL,
	}
}

func (l *Helper[businessData]) Start(ctx context.Context, spanName string) (context.Context, trace.Span) {
	return l.logger.trace.Start(ctx, spanName)
}

func (l *Helper[business]) InfoWithBusiness(ctx context.Context, val business) {
	m := &TagMapData{
		rw:   sync.RWMutex{},
		data: map[string]string{},
	}
	l.logWithBusiness(ctx, log.LevelInfo, val.Marshal(m))
}

func (l *Helper[business]) ErrorWithBusiness(ctx context.Context, val business) {
	m := &TagMapData{
		rw:   sync.RWMutex{},
		data: map[string]string{},
	}
	l.logWithBusiness(ctx, log.LevelError, val.Marshal(m))
}

func (l *Helper[businessData]) logWithBusiness(ctx context.Context, level log.Level, val []byte) {
	//业务日志写入文件
	l.logWithContext(ctx, level, val)
	//业务日志写入kafka->es 记录交易日志记录等
	l.logger.business(append(val, []byte("\n")...))
}

// 输出到普通日志文件
// 输出到jaeger
func (l *Helper[businessData]) logWithContext(ctx context.Context, level log.Level, msg interface{}) {
	l.log(level, msg)
	//输入到jaeger
	span := trace.SpanFromContext(ctx)
	val := ""
	switch v := msg.(type) {
	case fmt.Stringer:
		val = v.String()
	case string:
		val = v
	case byte:
		val = string(v)
	case error:
		val = v.Error()
	default:
		bf, _ := json.Marshal(v)
		val = string(bf)
	}
	span.AddEvent("msg_event", trace.WithAttributes(attribute.String("msg", val)))
}

func (l *Helper[business]) Debug(val interface{}) {
	l.log(log.LevelDebug, val)
}

func (l *Helper[business]) Debugf(format string, vals interface{}) {
	l.log(log.LevelDebug, fmt.Sprintf(format, vals))
}

func (l *Helper[business]) Info(val interface{}) {
	l.log(log.LevelInfo, val)
}

func (l *Helper[business]) Infof(format string, vals interface{}) {
	l.log(log.LevelInfo, fmt.Sprintf(format, vals))
}

func (l *Helper[businessData]) InfoWithContext(ctx context.Context, val interface{}) {
	l.logWithContext(ctx, log.LevelInfo, val)
}

func (l *Helper[businessData]) InfofWithContext(ctx context.Context) {

}

func (l *Helper[business]) Warn(val interface{}) {
	l.log(log.LevelWarn, val)
}

func (l *Helper[business]) Warnf(format string, vals interface{}) {
	l.log(log.LevelWarn, fmt.Sprintf(format, vals))
}

func (l *Helper[business]) Error(val interface{}) {
	l.log(log.LevelError, val)
}

func (l *Helper[business]) Errorf(format string, vals interface{}) {
	l.log(log.LevelError, fmt.Sprintf(format, vals))
}

func (l *Helper[business]) log(level log.Level, val interface{}) {
	switch v := val.(type) {
	case []byte:
		val = string(v)
	default:
	}
	l.logger.filelog(level, "msg", val)
}
