package singbox

import (
	"context"
	"net"

	"go.uber.org/zap"

	"github.com/shuffleman/mcte/pkg/config"
	"github.com/shuffleman/mcte/pkg/engine"
)

// RouteHandler sing-box 路由层入口。
type RouteHandler interface {
	RouteConnection(ctx context.Context, conn net.Conn, host string, port uint16) error
}

// Inbound MCTE 作为 sing-box inbound 的实现骨架。
type Inbound struct {
	opts   Options
	eng    *engine.Engine
	route  RouteHandler
	cancel context.CancelFunc
}

// New 构造 Inbound。
func New(opts Options, log *zap.Logger, route RouteHandler) (*Inbound, error) {
	c := config.Defaults()
	c.Listen.TCP = opts.Listen
	c.Listen.UDP = opts.ListenUDP
	if opts.Channel != "" {
		c.Tunnel.Channel = opts.Channel
	}
	if opts.UUIDField != "" {
		c.Tunnel.UUIDField = opts.UUIDField
	}
	if opts.TargetField != "" {
		c.Tunnel.TargetField = opts.TargetField
	}
	if len(opts.Fallback) > 0 {
		c.Fallback.Targets = opts.Fallback
	}
	if len(opts.FallbackTCP) > 0 {
		c.Fallback.TCP = opts.FallbackTCP
	}
	if len(opts.FallbackUDP) > 0 {
		c.Fallback.UDP = opts.FallbackUDP
	}
	if opts.MOTD != "" {
		c.Listen.MOTD = opts.MOTD
	}
	if opts.MaxSessions > 0 {
		c.Session.MaxConcurrent = opts.MaxSessions
	}
	for _, u := range opts.Users {
		c.Users = append(c.Users, config.UserConfig{Name: u.Name, UUID: u.UUID, Level: u.Level})
	}
	dialer := singUpstream{route: route}
	eng, err := engine.New(&c, log, dialer)
	if err != nil {
		return nil, err
	}
	return &Inbound{opts: opts, eng: eng, route: route}, nil
}

func (i *Inbound) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	i.cancel = cancel
	go func() { _ = i.eng.Run(ctx) }()
	return nil
}

func (i *Inbound) Close() error {
	if i.cancel != nil {
		i.cancel()
	}
	return nil
}

type singUpstream struct {
	route RouteHandler
}

func (s singUpstream) DialUpstream(ctx context.Context, host string, port uint16) (net.Conn, error) {
	a, b := net.Pipe()
	go func() {
		_ = s.route.RouteConnection(ctx, b, host, port)
	}()
	return a, nil
}
