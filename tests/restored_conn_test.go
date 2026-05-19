package tests

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"

	"github.com/shuffleman/mcte/pkg/detector"
)

// mockConn 仅用于 RestoredConn 测试，返回事先准备好的 tail 字节。
type mockConn struct {
	r io.Reader
}

func (m *mockConn) Read(p []byte) (int, error)         { return m.r.Read(p) }
func (m *mockConn) Write(p []byte) (int, error)        { return len(p), nil }
func (m *mockConn) Close() error                       { return nil }
func (m *mockConn) LocalAddr() net.Addr                { return nil }
func (m *mockConn) RemoteAddr() net.Addr               { return nil }
func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

func TestRestoredConnReread(t *testing.T) {
	head := []byte{1, 2, 3, 4, 5}
	tail := []byte{6, 7, 8, 9, 10}
	rc := detector.NewRestoredConn(head, &mockConn{r: bytes.NewReader(tail)})

	out := make([]byte, 10)
	n, err := io.ReadFull(rc, out)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if n != 10 {
		t.Fatalf("expected 10 bytes, got %d", n)
	}
	want := append(append([]byte{}, head...), tail...)
	if !bytes.Equal(out, want) {
		t.Fatalf("expected %v got %v", want, out)
	}
}
