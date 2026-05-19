package singbox

import "net"

// ConnAdapter sing-box Conn 接口适配。最小骨架。
type ConnAdapter struct{ net.Conn }

func NewConnAdapter(c net.Conn) *ConnAdapter { return &ConnAdapter{Conn: c} }
