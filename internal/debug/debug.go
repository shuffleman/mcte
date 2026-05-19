// Package debug 提供按环境变量开关的诊断日志。
//
// 启用方式：
//
//	export MCTE_DEBUG=1
//
// 默认完全静默，对协议指纹和性能零影响。
package debug

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var (
	enabled atomic.Bool
	once    sync.Once
)

func ensureInit() {
	once.Do(func() {
		v := os.Getenv("MCTE_DEBUG")
		if v == "1" || v == "true" || v == "yes" {
			enabled.Store(true)
		}
	})
}

// Enabled 检查 debug 是否已启用。
func Enabled() bool {
	ensureInit()
	return enabled.Load()
}

// Logf 仅在启用时打印；带毫秒精度时间戳。
func Logf(format string, args ...any) {
	if !Enabled() {
		return
	}
	fmt.Fprintf(os.Stderr, "[mcte %s] %s\n",
		time.Now().Format("15:04:05.000"),
		fmt.Sprintf(format, args...))
}
