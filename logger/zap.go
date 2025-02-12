package logger

import (
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func getLogField(keyvals ...interface{}) []zapcore.Field {
	keylen := len(keyvals)
	if keylen == 0 || keylen%2 != 0 {
		return nil
	}
	fields := make([]zapcore.Field, 0, keylen/2)
	for i := 0; i < keylen; i += 2 {
		fields = append(fields, zap.Any(fmt.Sprint(keyvals[i]), keyvals[i+1]))
	}
	return fields
}
