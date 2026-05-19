package tests

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shuffleman/mcte/internal/netutil"
)

// slowConn 模拟一个"卡死的写者"+"持续工作的写者"。
// 调用方通过 stuckN 控制让前 stuckN 次写阻塞，后续写立即返回。
type slowConn struct {
	stuckN   atomic.Int32
	stuckCh  chan struct{}
	closed   atomic.Bool
}

func newSlowConn(stuckN int32) *slowConn {
	c := &slowConn{stuckCh: make(chan struct{})}
	c.stuckN.Store(stuckN)
	return c
}

func (s *slowConn) Write(p []byte) (int, error) {
	if s.closed.Load() {
		return 0, net.ErrClosed
	}
	if s.stuckN.Load() > 0 {
		s.stuckN.Add(-1)
		<-s.stuckCh // 阻塞直到 close
		return 0, net.ErrClosed
	}
	return len(p), nil
}

func (s *slowConn) Read(p []byte) (int, error) {
	if s.closed.Load() {
		return 0, net.ErrClosed
	}
	time.Sleep(time.Hour)
	return 0, nil
}

func (s *slowConn) Close() error {
	if s.closed.CompareAndSwap(false, true) {
		// 通知所有卡住的 Write 退出
		close(s.stuckCh)
	}
	return nil
}

func (s *slowConn) LocalAddr() net.Addr                { return nil }
func (s *slowConn) RemoteAddr() net.Addr               { return nil }
func (s *slowConn) SetDeadline(t time.Time) error      { return nil }
func (s *slowConn) SetReadDeadline(t time.Time) error  { return nil }
func (s *slowConn) SetWriteDeadline(t time.Time) error { return nil }

// 单 goroutine 卡死场景：WatchdogConn 应在 timeout+tick 内 close。
func TestWatchdogSingleStuckWriter(t *testing.T) {
	raw := newSlowConn(1) // 第一次写卡住
	wd := netutil.NewWatchdogConn(raw, 300*time.Millisecond, 50*time.Millisecond)
	wd.Start(context.Background())
	defer wd.Stop()

	done := make(chan error, 1)
	go func() {
		_, err := wd.Write([]byte("blocked forever"))
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil || !errors.Is(err, net.ErrClosed) {
			t.Fatalf("expected ErrClosed after watchdog close, got %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("watchdog did not close stuck write within 1s")
	}
}

// 多 writer 关键场景：一个 goroutine 卡死、另一个高频成功写。
// TimeoutConn 风格会因为高频写不断 SetDeadline 而永远不触发；
// WatchdogConn 应只看"最早一个 in-flight write"，正确检出。
func TestWatchdogStuckAmidActiveTraffic(t *testing.T) {
	raw := newSlowConn(1) // 只让第一次写卡，其余立即成功
	wd := netutil.NewWatchdogConn(raw, 300*time.Millisecond, 50*time.Millisecond)
	wd.Start(context.Background())
	defer wd.Stop()

	// 卡死的写者
	stuckDone := make(chan error, 1)
	go func() {
		_, err := wd.Write([]byte("stuck"))
		stuckDone <- err
	}()

	// 高频成功的写者（模拟 KeepAlive 每 50ms 一次）
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		t := time.NewTicker(50 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				_, _ = wd.Write([]byte("ka"))
			}
		}
	}()

	select {
	case err := <-stuckDone:
		if err == nil || !errors.Is(err, net.ErrClosed) {
			t.Fatalf("expected ErrClosed, got %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("watchdog failed to detect stuck writer amid active traffic")
	}
}

// 全部写都正常时不应误关。
func TestWatchdogNoFalsePositive(t *testing.T) {
	raw := newSlowConn(0) // 没有任何写卡
	wd := netutil.NewWatchdogConn(raw, 200*time.Millisecond, 30*time.Millisecond)
	wd.Start(context.Background())
	defer wd.Stop()

	for i := 0; i < 50; i++ {
		if _, err := wd.Write([]byte("ok")); err != nil {
			t.Fatalf("unexpected error at iter %d: %v", i, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
