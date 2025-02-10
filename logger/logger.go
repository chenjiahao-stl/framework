package logger

import (
	"fmt"
	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
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

// Logger 日志组件
type Logger struct {
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

//func NewLogger() *Logger {
//	l := &Logger{}
//	config := zap.NewProductionConfig()
//	zap.NewProductionEncoderConfig()
//	log, err := config.Build()
//	zap.ReplaceGlobals(log)
//	logger, _ := zap.NewProduction()
//	return l
//}

type logger struct {
	*zap.Logger
}

// NewLogger 创建并初始化 zap logger，结合 lumberjack 进行日志切割
func NewLogger(config LogConfig) (*logger, error) {
	zapLogger, err := initZapLogger(config)
	if err != nil {
		return nil, err
	}
	//var cfg zap.Config
	//logger := zap.Must(cfg.Build())
	l := &logger{
		zapLogger,
	}

	// 确保日志被正确刷新
	return l, nil
}

func initLumberjack(config LogConfig) *lumberjack.Logger {
	// 定义日志输出的配置
	writer := &lumberjack.Logger{
		Filename:   fmt.Sprintf("%s/%s.log", config.LogDir, config.LogName),
		MaxSize:    config.MaxSize,    // 每个日志文件最大尺寸,megabytes，M 为单位
		MaxBackups: config.MaxBackups, // 保留旧文件最大份数
		MaxAge:     config.MaxAge,     // days  旧文件最大保存天数
		Compress:   config.Compress,   // 是否压缩日志归档，默认不压缩
	}
	return writer
}

func initZapLogger(config LogConfig) (*zap.Logger, error) {
	writer := initLumberjack(config)
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
			zap.NewAtomicLevelAt(zapcore.DebugLevel), // 开发环境显示 Debug 级别及以上日志
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
			zap.NewAtomicLevelAt(zapcore.InfoLevel), // 生产环境只输出 Info 级别及以上日志
		)
	}

	// 创建 zap logger
	// zap.AddCaller 输出文件名和行号
	// zap.AddStacktrace 输出调用堆栈
	zapL := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.FatalLevel))
	return zapL, nil
}

// Sync 刷新日志
func Sync(logger *zap.Logger) {
	if err := logger.Sync(); err != nil {
		fmt.Printf("Error syncing logger: %v", err)
	}
}
