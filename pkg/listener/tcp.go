// Package listener 监听 TCP / UDP 并提供统一 conn channel。
package listener

import (
	"context"
	"net"
	"time"
)

// TCPListener 维护 accept loop 并将 conn 投递到 channel。
type TCPListener struct {
	ln net.Listener
	ch chan net.Conn
}

func NewTCPListener(addr string, queue int) (*TCPListener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	if queue <= 0 {
		queue = 256
	}
	return &TCPListener{ln: ln, ch: make(chan net.Conn, queue)}, nil
}

// Run accept loop；ctx 取消时退出并关闭 listener。
func (t *TCPListener) Run(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		_ = t.ln.Close()
	}()
	for {
		c, err := t.ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		// 开启 TCP keepalive，30s 探测；防止半开连接让 io.Copy 永久阻塞
		if tc, ok := c.(*net.TCPConn); ok {
			_ = tc.SetKeepAlive(true)
			_ = tc.SetKeepAlivePeriod(30 * time.Second)
		}
		select {
		case t.ch <- c:
		case <-ctx.Done():
			_ = c.Close()
			return nil
		}
	}
}

func (t *TCPListener) Accept() <-chan net.Conn { return t.ch }
func (t *TCPListener) Addr() net.Addr           { return t.ln.Addr() }
