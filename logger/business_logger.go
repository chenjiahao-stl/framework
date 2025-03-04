package logger

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

type TagMapData struct {
	rw   sync.RWMutex
	data map[string]string
}

// BusinessData 定义一个日志打印的约定接口
type BusinessData interface {
	Marshal(m *TagMapData) []byte
}

type BusinessStep struct {
	Step string
	Msg  interface{}
}

func (b BusinessStep) Marshal(m *TagMapData) []byte {
	m.rw.Lock()
	defer m.rw.Unlock()
	if b.Step != "" {
		m.data["step"] = b.Step
	}

	msg := ""
	switch v := b.Msg.(type) {
	case fmt.Stringer:
		msg = v.String()
	case string:
		msg = v
	case []byte:
		msg = string(v)
	case error:
		msg = v.Error()
	default:
		bf, _ := json.Marshal(v)
		msg = string(bf)
	}
	m.data["commont"] = msg
	m.data["time"] = time.Now().Format(time.RFC3339Nano)
	bf, _ := json.Marshal(m.data)
	return bf
}
