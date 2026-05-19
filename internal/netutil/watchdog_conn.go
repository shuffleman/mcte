package netutil

import (
	"context"
	"net"
	"sync/atomic"
	"time"
)

// WatchdogConn 通过独立后台 goroutine 监测 Write 阻塞时长，
// 取代 TimeoutConn 每次写都 SetDeadline 的做法。
//
// 为什么需要它：
//   TimeoutConn.Write 在每次写前刷新 deadline。当多个 goroutine 共享同一个 conn
//   并并发写时，SetWriteDeadline 是 per-conn 全局状态，任一 goroutine 的写都会
//   把 deadline 推后；如果存在周期性写（KeepAlive）周期 < WriteTimeout，
//   那么真正卡死的 Write 的 deadline 永远不会到期。
//
// WatchdogConn 的方案：
//   - Write 进入时 atomic 自增 inflight 计数；如从 0→1 则记录 startedAt
//   - Write 退出时 atomic 自减；如降到 0 则清零 startedAt
//   - 后台 goroutine 每 tick 检查：inflight>0 且 now-startedAt>timeout → 强 Close conn
//   - 任何一次卡死的 Write 都会让 conn 在 timeout+tick 内被强关，下游 io 全部返回 net.ErrClosed
type WatchdogConn struct {
	net.Conn
	timeout     time.Duration
	tick        time.Duration
	inflight    atomic.Int32
	startedAt   atomic.Int64 // unix nano；0 表示当前无 in-flight write
	closed      atomic.Bool
	stop        chan struct{}
	stoppedOnce atomic.Bool
}

// NewWatchdogConn 包装 conn。timeout=0 时取 30s，tick=0 时取 timeout/6（最少 1s）。
func NewWatchdogConn(c net.Conn, timeout, tick time.Duration) *WatchdogConn {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	if tick <= 0 {
		tick = timeout / 6
		if tick < time.Second {
			tick = time.Second
		}
	}
	w := &WatchdogConn{Conn: c, timeout: timeout, tick: tick, stop: make(chan struct{})}
	return w
}

// Start 在独立 goroutine 中运行 watchdog。
// 一般在 ctx 结束或 conn 关闭时停止；调用方应在 handler 退出时 Stop。
func (w *WatchdogConn) Start(ctx context.Context) {
	go w.run(ctx)
}

// Stop 显式停止 watchdog（幂等）。Close 也会自动停止。
func (w *WatchdogConn) Stop() {
	if w.stoppedOnce.CompareAndSwap(false, true) {
		close(w.stop)
	}
}

func (w *WatchdogConn) run(ctx context.Context) {
	t := time.NewTicker(w.tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stop:
			return
		case <-t.C:
			started := w.startedAt.Load()
			if started == 0 {
				continue
			}
			if time.Since(time.Unix(0, started)) > w.timeout {
				// 卡死超时：强 close。后续 Write/Read 会拿到 net.ErrClosed。
				w.closed.Store(true)
				_ = w.Conn.Close()
				return
			}
		}
	}
}

// Write 进入计数后调底层 Write。
func (w *WatchdogConn) Write(p []byte) (int, error) {
	if w.closed.Load() {
		return 0, net.ErrClosed
	}
	if w.inflight.Add(1) == 1 {
		w.startedAt.Store(time.Now().UnixNano())
	}
	n, err := w.Conn.Write(p)
	if w.inflight.Add(-1) == 0 {
		w.startedAt.Store(0)
	}
	return n, err
}

// Close 强关并停止 watchdog。
func (w *WatchdogConn) Close() error {
	w.closed.Store(true)
	w.Stop()
	return w.Conn.Close()
}
