package xray

import (
	"net"
	"time"
)

// ConnAdapter 实现 net.Conn，将 MCTE tunnel bridge 的上游端对接到 Xray。
// 这里给出一个最小骨架；与具体 Xray 内部接口对接由 register.go 完成。
type ConnAdapter struct {
	net.Conn
	deadline time.Time
}

func NewConnAdapter(c net.Conn) *ConnAdapter { return &ConnAdapter{Conn: c} }
