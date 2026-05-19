// Package passthrough 实现真实 MC 客户端到后端的透明转发。
package passthrough

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"time"
)

// Dialer 轮询式拨号后端 MC 列表。
type Dialer struct {
	targets []string
	timeout time.Duration
	idx     atomic.Uint32
}

// NewDialer 构造。
func NewDialer(targets []string, timeout time.Duration) (*Dialer, error) {
	if len(targets) == 0 {
		return nil, errors.New("passthrough: empty targets")
	}
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	return &Dialer{targets: targets, timeout: timeout}, nil
}

// Dial 选一个目标拨号；若失败则按顺序回退到下一个。
// 拨号后启用 TCP keepalive，防止半开连接让透传 io.Copy 永久阻塞。
func (d *Dialer) Dial(ctx context.Context) (net.Conn, string, error) {
	n := uint32(len(d.targets))
	start := d.idx.Add(1) - 1
	dialer := &net.Dialer{
		KeepAlive: 30 * time.Second, // 启用 SO_KEEPALIVE
	}
	var lastErr error
	for i := uint32(0); i < n; i++ {
		t := d.targets[(start+i)%n]
		dctx, cancel := context.WithTimeout(ctx, d.timeout)
		c, err := dialer.DialContext(dctx, "tcp", t)
		cancel()
		if err == nil {
			return c, t, nil
		}
		lastErr = err
	}
	return nil, "", lastErr
}
