package xray

import (
	"context"
	"net"

	"go.uber.org/zap"

	"github.com/shuffleman/mcte/pkg/config"
	"github.com/shuffleman/mcte/pkg/engine"
)

// Listener Xray 侧的 inbound 入口。
type Listener struct {
	eng    *engine.Engine
	handle func(net.Conn)
	cancel context.CancelFunc
}

// NewListener 用 MCTEConfig 构造。handle 为 Xray ConnHandler。
func NewListener(cfg MCTEConfig, log *zap.Logger, handle func(net.Conn)) (*Listener, error) {
	c := config.Defaults()
	c.Listen.TCP = cfg.ListenTCP
	c.Listen.UDP = cfg.ListenUDP
	if cfg.Channel != "" {
		c.Tunnel.Channel = cfg.Channel
	}
	if cfg.UUIDField != "" {
		c.Tunnel.UUIDField = cfg.UUIDField
	}
	if cfg.TargetField != "" {
		c.Tunnel.TargetField = cfg.TargetField
	}
	if len(cfg.Fallback) > 0 {
		c.Fallback.Targets = cfg.Fallback
	}
	if len(cfg.FallbackTCP) > 0 {
		c.Fallback.TCP = cfg.FallbackTCP
	}
	if len(cfg.FallbackUDP) > 0 {
		c.Fallback.UDP = cfg.FallbackUDP
	}
	if cfg.MOTD != "" {
		c.Listen.MOTD = cfg.MOTD
	}
	if cfg.MaxSessions > 0 {
		c.Session.MaxConcurrent = cfg.MaxSessions
	}
	if cfg.Mimic {
		c.Tunnel.Mimic.Enabled = true
	}
	if cfg.S2CRate > 0 {
		c.Tunnel.S2CRateBytesPerSec = cfg.S2CRate
	}
	for _, u := range cfg.Users {
		c.Users = append(c.Users, config.UserConfig{Name: u.Name, UUID: u.UUID, Level: u.Level})
	}
	dialer := xrayUpstream{handle: handle}
	eng, err := engine.New(&c, log, dialer)
	if err != nil {
		return nil, err
	}
	return &Listener{eng: eng, handle: handle}, nil
}

// Start 启动 Engine。
func (l *Listener) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	l.cancel = cancel
	go func() { _ = l.eng.Run(ctx) }()
	return nil
}

// Close 关闭。
func (l *Listener) Close() error {
	if l.cancel != nil {
		l.cancel()
	}
	return nil
}

// xrayUpstream 通过 net.Pipe 把隧道协商出来的 host:port 交给 Xray ConnHandler。
type xrayUpstream struct {
	handle func(net.Conn)
}

func (x xrayUpstream) DialUpstream(_ context.Context, host string, port uint16) (net.Conn, error) {
	a, b := net.Pipe()
	go x.handle(taggedConn{Conn: b, host: host, port: port})
	return a, nil
}

type taggedConn struct {
	net.Conn
	host string
	port uint16
}

func (t taggedConn) Host() string { return t.host }
func (t taggedConn) Port() uint16 { return t.port }
