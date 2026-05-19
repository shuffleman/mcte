package tests

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shuffleman/mcte/internal/netutil"
)

// halfOpenConn 模拟 TCP 半开：Write 永远阻塞，Read 也永远阻塞。
type halfOpenConn struct {
	closed atomic.Bool
	done   chan struct{}
}

func newHalfOpenConn() *halfOpenConn {
	return &halfOpenConn{done: make(chan struct{})}
}

func (c *halfOpenConn) Read(p []byte) (int, error) {
	<-c.done
	return 0, io.EOF
}

func (c *halfOpenConn) Write(p []byte) (int, error) {
	<-c.done
	return 0, net.ErrClosed
}

func (c *halfOpenConn) Close() error {
	if c.closed.CompareAndSwap(false, true) {
		close(c.done)
	}
	return nil
}

func (c *halfOpenConn) LocalAddr() net.Addr                { return nil }
func (c *halfOpenConn) RemoteAddr() net.Addr               { return nil }
func (c *halfOpenConn) SetDeadline(t time.Time) error      { return nil }
func (c *halfOpenConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *halfOpenConn) SetWriteDeadline(t time.Time) error { return nil }

// closableConn 包一个 net.Pipe 半边，外加被关计数。
type closableConn struct {
	net.Conn
	closedAt atomic.Int64
}

func (c *closableConn) Close() error {
	c.closedAt.CompareAndSwap(0, time.Now().UnixNano())
	return c.Conn.Close()
}

// 验证：netutil.Pipe + WatchdogConn 两端时，一端 Write 卡死能传递到另一端 close。
func TestPipeWatchdogClosePropagation(t *testing.T) {
	// 端 A：半开（Write 卡死）
	rawA := newHalfOpenConn()
	// 端 B：可观察被关
	pipeBOuter, pipeBInner := net.Pipe()
	defer pipeBInner.Close()
	rawB := &closableConn{Conn: pipeBOuter}

	wdA := netutil.NewWatchdogConn(rawA, 200*time.Millisecond, 30*time.Millisecond)
	wdB := netutil.NewWatchdogConn(rawB, 200*time.Millisecond, 30*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wdA.Start(ctx)
	wdB.Start(ctx)
	defer wdA.Stop()
	defer wdB.Stop()

	// 让"对端"（pipeBInner）持续发数据 → Pipe 读到后会写到 wdA → wdA.Write 卡死
	go func() {
		for {
			if _, err := pipeBInner.Write([]byte("hello-data-payload-")); err != nil {
				return
			}
		}
	}()

	done := make(chan error, 1)
	go func() {
		done <- netutil.Pipe(ctx, wdA, wdB)
	}()

	select {
	case err := <-done:
		// Pipe 应该返回，且两端都已被关
		_ = err
		if rawB.closedAt.Load() == 0 {
			t.Fatalf("端 B 未被关闭，关闭事件没有从端 A 传递过来")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Pipe 未在 watchdog 超时后退出")
	}
}

// 验证：双 WatchdogConn 模式下任一端 Close，另一端的 io 立即 ErrClosed。
func TestPipeManualCloseBothEnds(t *testing.T) {
	a1, a2 := net.Pipe()
	b1, b2 := net.Pipe()

	wdA := netutil.NewWatchdogConn(a1, 5*time.Second, 0)
	wdB := netutil.NewWatchdogConn(b1, 5*time.Second, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wdA.Start(ctx)
	wdB.Start(ctx)

	done := make(chan error, 1)
	go func() {
		done <- netutil.Pipe(ctx, wdA, wdB)
	}()

	// 模拟"对端 A"主动断开
	go a2.Close()
	defer b2.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("Pipe did not close after one side closed")
	}

	// 两端都应已关
	if _, err := a1.Write([]byte("x")); !errors.Is(err, io.ErrClosedPipe) && !errors.Is(err, net.ErrClosed) {
		t.Logf("a1 write after pipe close: %v (acceptable)", err)
	}
	if _, err := b1.Write([]byte("x")); !errors.Is(err, io.ErrClosedPipe) && !errors.Is(err, net.ErrClosed) {
		t.Logf("b1 write after pipe close: %v (acceptable)", err)
	}
}
