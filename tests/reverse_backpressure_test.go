package tests

import (
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shuffleman/mcte/internal/netutil"
)

// blackholeWriter 模拟 TCP buf 满：Write 永远阻塞，直到 SetWriteDeadline 触发超时。
type blackholeConn struct {
	net.Conn
	deadline atomic.Value // time.Time
	closed   atomic.Bool
}

func newBlackholeConn() *blackholeConn {
	return &blackholeConn{}
}

func (b *blackholeConn) Read(p []byte) (int, error) {
	if b.closed.Load() {
		return 0, net.ErrClosed
	}
	time.Sleep(time.Hour)
	return 0, io.EOF
}

func (b *blackholeConn) Write(p []byte) (int, error) {
	if b.closed.Load() {
		return 0, net.ErrClosed
	}
	for {
		if b.closed.Load() {
			return 0, net.ErrClosed
		}
		dv := b.deadline.Load()
		if dv != nil {
			if d, ok := dv.(time.Time); ok && !d.IsZero() {
				if time.Now().After(d) {
					return 0, errTimeout{}
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (b *blackholeConn) Close() error {
	b.closed.Store(true)
	return nil
}

func (b *blackholeConn) LocalAddr() net.Addr  { return nil }
func (b *blackholeConn) RemoteAddr() net.Addr { return nil }

func (b *blackholeConn) SetDeadline(t time.Time) error {
	b.deadline.Store(t)
	return nil
}

func (b *blackholeConn) SetReadDeadline(t time.Time) error  { return nil }
func (b *blackholeConn) SetWriteDeadline(t time.Time) error { b.deadline.Store(t); return nil }

type errTimeout struct{}

func (errTimeout) Error() string   { return "timeout" }
func (errTimeout) Timeout() bool   { return true }
func (errTimeout) Temporary() bool { return true }

// TimeoutConn 写阻塞时应超时返回错误，而不是永远卡死。
func TestTimeoutConnWriteUnblock(t *testing.T) {
	raw := newBlackholeConn()
	tc := &netutil.TimeoutConn{Conn: raw, WriteTimeout: 200 * time.Millisecond}

	done := make(chan error, 1)
	go func() {
		_, err := tc.Write([]byte("hello"))
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("expected timeout error, got nil")
		}
		var te interface{ Timeout() bool }
		if !errors.As(err, &te) || !te.Timeout() {
			t.Fatalf("expected timeout error, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("write did not unblock within 2s")
	}
}

// TimeoutConn WriteTimeout=0 时不设 deadline，保持原有阻塞语义。
func TestTimeoutConnZeroTimeoutBlocks(t *testing.T) {
	raw := newBlackholeConn()
	tc := &netutil.TimeoutConn{Conn: raw, WriteTimeout: 0}

	done := make(chan error, 1)
	go func() {
		_, err := tc.Write([]byte("hello"))
		done <- err
	}()

	select {
	case <-done:
		t.Fatalf("write returned with WriteTimeout=0")
	case <-time.After(200 * time.Millisecond):
		// 期望仍在阻塞
		raw.Close()
	}
	<-done
}
