package listener

import (
	"context"
	"net"

	"github.com/shuffleman/mcte/pkg/protocol/bedrock/raknet"
)

// Hybrid 同时启动 TCP 与 UDP；对外提供两个 channel，engine 分别消费。
type Hybrid struct {
	tcp *TCPListener
	udp *UDPListener
}

// New 构造，任一地址为空字符串则跳过对应 listener。
func New(tcpAddr, udpAddr string) (*Hybrid, error) {
	return NewWithOptions(tcpAddr, udpAddr, Options{})
}

// NewWithOptions 同 New，但允许注入 UDP 监听选项（max sessions 等）。
func NewWithOptions(tcpAddr, udpAddr string, udpOpt Options) (*Hybrid, error) {
	h := &Hybrid{}
	if tcpAddr != "" {
		t, err := NewTCPListener(tcpAddr, 256)
		if err != nil {
			return nil, err
		}
		h.tcp = t
	}
	if udpAddr != "" {
		u, err := NewUDPListener(udpAddr, 256, udpOpt)
		if err != nil {
			return nil, err
		}
		h.udp = u
	}
	return h, nil
}

// Run 启动两端，阻塞至 ctx 取消。
func (h *Hybrid) Run(ctx context.Context) error {
	errCh := make(chan error, 2)
	if h.tcp != nil {
		go func() { errCh <- h.tcp.Run(ctx) }()
	}
	if h.udp != nil {
		go func() { errCh <- h.udp.Run(ctx) }()
	}
	<-ctx.Done()
	return nil
}

// AcceptTCP TCP 入口；nil 表示未启用。
func (h *Hybrid) AcceptTCP() <-chan net.Conn {
	if h.tcp == nil {
		return nil
	}
	return h.tcp.Accept()
}

// AcceptUDP UDP 入口；nil 表示未启用。
func (h *Hybrid) AcceptUDP() <-chan *raknet.Session {
	if h.udp == nil {
		return nil
	}
	return h.udp.Accept()
}
