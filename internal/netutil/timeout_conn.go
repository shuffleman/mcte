package netutil

import (
	"net"
	"time"
)

// TimeoutConn 在每次读写前刷新 deadline。
type TimeoutConn struct {
	net.Conn
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

func (c *TimeoutConn) Read(p []byte) (int, error) {
	if c.ReadTimeout > 0 {
		_ = c.Conn.SetReadDeadline(time.Now().Add(c.ReadTimeout))
	}
	return c.Conn.Read(p)
}

func (c *TimeoutConn) Write(p []byte) (int, error) {
	if c.WriteTimeout > 0 {
		_ = c.Conn.SetWriteDeadline(time.Now().Add(c.WriteTimeout))
	}
	return c.Conn.Write(p)
}
