package passthrough

import (
	"context"
	"net"
	"time"

	"github.com/shuffleman/mcte/internal/netutil"
)

// HandlerConfig 透传 handler 配置。
type HandlerConfig struct {
	// WriteTimeout 任一端 Write 持续阻塞超过此时长则强关连接。
	// 只覆盖 Write 路径，不影响玩家长时间挂机不发包（那是 Read 阻塞，正常）。
	WriteTimeout time.Duration
}

// Handler 接收已识别为 MC 客户端的连接，连接后端并双向透传。
type Handler struct {
	dialer *Dialer
	cfg    HandlerConfig
}

func NewHandler(d *Dialer) *Handler {
	return NewHandlerWithConfig(d, HandlerConfig{})
}

func NewHandlerWithConfig(d *Dialer, cfg HandlerConfig) *Handler {
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 30 * time.Second
	}
	return &Handler{dialer: d, cfg: cfg}
}

// Handle 透传 client 到后端。
//
// 两端均用 WatchdogConn 包装：任一端 Write 持续阻塞超过 WriteTimeout 即强 Close
// 该端 conn → 对应方向 io.Copy 立即返回 ErrClosed → netutil.Pipe 内 closeBoth
// 关闭另一端 → 反方向 io.Copy 也退出。任一端断开都会传递到另一端。
func (h *Handler) Handle(ctx context.Context, client net.Conn) error {
	backend, _, err := h.dialer.Dial(ctx)
	if err != nil {
		_ = client.Close()
		return err
	}

	cw := netutil.NewWatchdogConn(client, h.cfg.WriteTimeout, 0)
	bw := netutil.NewWatchdogConn(backend, h.cfg.WriteTimeout, 0)
	cw.Start(ctx)
	bw.Start(ctx)
	defer cw.Stop()
	defer bw.Stop()

	return netutil.Pipe(ctx, cw, bw)
}
