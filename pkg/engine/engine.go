// Package engine 是 MCTE 根对象：组装并驱动所有模块。
package engine

import (
	"context"
	"errors"
	"net"
	"time"

	"go.uber.org/zap"

	"github.com/shuffleman/mcte/pkg/auth"
	"github.com/shuffleman/mcte/pkg/config"
	"github.com/shuffleman/mcte/pkg/detector"
	"github.com/shuffleman/mcte/pkg/listener"
	"github.com/shuffleman/mcte/pkg/passthrough"
	"github.com/shuffleman/mcte/pkg/plugin"
	"github.com/shuffleman/mcte/pkg/plugin/builtin/metrics"
	"github.com/shuffleman/mcte/pkg/protocol/bedrock/raknet"
	"github.com/shuffleman/mcte/pkg/session"
	"github.com/shuffleman/mcte/pkg/tunnel"
)

// UpstreamDialer 由集成层提供：将协商出的 host:port 交给 Xray / sing-box 路由层。
type UpstreamDialer interface {
	DialUpstream(ctx context.Context, host string, port uint16) (net.Conn, error)
}

// DefaultUpstreamDialer 直接 TCP 拨号。
type DefaultUpstreamDialer struct{ Timeout time.Duration }

func (d DefaultUpstreamDialer) DialUpstream(ctx context.Context, host string, port uint16) (net.Conn, error) {
	addr := net.JoinHostPort(host, itoa(port))
	dctx, cancel := context.WithTimeout(ctx, d.Timeout)
	defer cancel()
	return (&net.Dialer{KeepAlive: 30 * time.Second}).DialContext(dctx, "tcp", addr)
}

// Engine 主体。
type Engine struct {
	cfg      *config.Config
	log      *zap.Logger
	hybrid   *listener.Hybrid
	pass     *passthrough.Handler
	tun      *tunnel.Handler
	tunBE    *tunnel.BedrockHandler
	passBE   *passthrough.BedrockHandler
	mgr      *session.Manager
	registry *plugin.Registry
	metrics  *metrics.Plugin

	validator   *auth.Validator
	uuidField   string
	targetField string

	// accept 级总并发限流：accept 后立即取 token，handle 完归还。
	sem chan struct{}
}

// New 构造 Engine。upstream 为 nil 时使用 DefaultUpstreamDialer。
func New(cfg *config.Config, log *zap.Logger, upstream UpstreamDialer) (*Engine, error) {
	if cfg == nil {
		return nil, errors.New("engine: nil config")
	}
	if log == nil {
		log = zap.NewNop()
	}
	if upstream == nil {
		upstream = DefaultUpstreamDialer{Timeout: 10 * time.Second}
	}

	hy, err := listener.NewWithOptions(cfg.Listen.TCP, cfg.Listen.UDP, listener.Options{
		MaxSessions: cfg.Session.MaxConcurrent,
		IdleTimeout: cfg.Session.IdleTimeout,
	})
	if err != nil {
		return nil, err
	}
	fallbackTargets := cfg.FallbackTargets()
	fallbackTimeout := cfg.FallbackDialTimeout()
	d, err := passthrough.NewDialer(fallbackTargets, fallbackTimeout)
	if err != nil {
		return nil, err
	}
	passH := passthrough.NewHandler(d)
	var tunMimic *tunnel.MimicMatcher
	if cfg.Tunnel.Mimic.Enabled {
		tunMimic = buildMimicMatcher(cfg.Tunnel.Mimic)
	}
	tunH := tunnel.NewHandler(tunnel.HandlerConfig{
		DataChannel:      cfg.Tunnel.Channel,
		WriteTimeout:     cfg.Tunnel.WriteTimeout,
		KeepAliveEvery:   cfg.Tunnel.KeepAliveEvery,
		KeepAliveTimeout: cfg.Tunnel.KeepAliveTimeout,
		Mimic:            tunMimic,
	}, func(ctx context.Context, host string, port uint16) (net.Conn, error) {
		return upstream.DialUpstream(ctx, host, port)
	})
	tunBE := tunnel.NewBedrockHandler(tunnel.HandlerConfig{
		WriteTimeout:     cfg.Tunnel.WriteTimeout,
		KeepAliveEvery:   cfg.Tunnel.KeepAliveEvery,
		KeepAliveTimeout: cfg.Tunnel.KeepAliveTimeout,
	}, func(ctx context.Context, host string, port uint16) (net.Conn, error) {
		return upstream.DialUpstream(ctx, host, port)
	})
	bdDialer, derr := passthrough.NewBedrockDialer(fallbackTargets, fallbackTimeout)
	if derr != nil {
		return nil, derr
	}
	passBE := passthrough.NewBedrockHandler(bdDialer)

	// 构造 user validator
	validator := auth.NewValidator()
	for _, u := range cfg.Users {
		id, perr := auth.ParseUUID(u.UUID)
		if perr != nil {
			return nil, errors.New("engine: user '" + u.Name + "' bad uuid: " + perr.Error())
		}
		if aerr := validator.Add(&auth.User{Name: u.Name, UUID: id, Level: u.Level}); aerr != nil {
			return nil, aerr
		}
	}

	mgr := session.NewManager(cfg.Session.IdleTimeout, cfg.Session.MaxConcurrent)
	reg := plugin.NewRegistry()

	var metricsP *metrics.Plugin
	if cfg.Metrics.Enabled {
		metricsP = metrics.New(cfg.Metrics.Listen)
		reg.Register(metricsP)
	}

	maxConn := cfg.Session.MaxConcurrent
	if maxConn <= 0 {
		maxConn = 10000
	}
	return &Engine{
		cfg:         cfg,
		log:         log,
		hybrid:      hy,
		pass:        passH,
		tun:         tunH,
		tunBE:       tunBE,
		passBE:      passBE,
		mgr:         mgr,
		registry:    reg,
		metrics:     metricsP,
		validator:   validator,
		uuidField:   cfg.Tunnel.UUIDField,
		targetField: cfg.Tunnel.TargetField,
		sem:         make(chan struct{}, maxConn),
	}, nil
}

// Run 启动 Engine，阻塞至 ctx 取消。
func (e *Engine) Run(ctx context.Context) error {
	if err := e.registry.Start(ctx); err != nil {
		return err
	}
	defer e.registry.Stop(context.Background())

	go e.mgr.RunSweeper(ctx, func(s *session.Session) {
		e.registry.FireSession(s, plugin.SessionClose)
	})
	go func() { _ = e.hybrid.Run(ctx) }()

	tcpCh := e.hybrid.AcceptTCP()
	udpCh := e.hybrid.AcceptUDP()
	for {
		select {
		case <-ctx.Done():
			return nil
		case c, ok := <-tcpCh:
			if !ok {
				tcpCh = nil
				continue
			}
			select {
			case e.sem <- struct{}{}:
				go func() {
					defer func() { <-e.sem }()
					e.handleTCP(ctx, c)
				}()
			default:
				e.log.Debug("tcp accept dropped: capacity full")
				_ = c.Close()
			}
		case s, ok := <-udpCh:
			if !ok {
				udpCh = nil
				continue
			}
			select {
			case e.sem <- struct{}{}:
				go func() {
					defer func() { <-e.sem }()
					e.handleUDP(ctx, s)
				}()
			default:
				e.log.Debug("udp accept dropped: capacity full")
				_ = s.Close()
			}
		}
	}
}

func (e *Engine) handleTCP(ctx context.Context, c net.Conn) {
	res, err := detector.Detect(c, detector.Options{
		Validator: e.validator,
		Deadline:  5 * time.Second,
	})
	if err != nil {
		e.log.Debug("detect failed", zap.Error(err))
		return
	}

	s := session.Acquire()
	defer session.Release(s)
	s.ID = e.mgr.NewID()
	s.Kind = res.Kind
	s.Proto = detector.ProtoJava
	if res.Handshake != nil {
		s.Version = res.Handshake.ProtocolVersion
	}
	s.RemoteAddr = c.RemoteAddr()
	s.CreatedAt = time.Now()
	s.Touch()
	if !e.mgr.Add(s) {
		_ = c.Close()
		return
	}
	defer e.mgr.Remove(s.ID)
	e.registry.FireSession(s, plugin.SessionOpen)
	defer e.registry.FireSession(s, plugin.SessionClose)

	switch res.Kind {
	case detector.KindTunnel:
		if err := e.tun.Handle(ctx, res.Conn, res); err != nil {
			e.log.Debug("java tunnel ended", zap.Error(err))
		}
	default:
		if err := e.pass.Handle(ctx, res.Conn); err != nil {
			e.log.Debug("java passthrough ended", zap.Error(err))
		}
	}
}

func (e *Engine) handleUDP(ctx context.Context, sess *raknet.Session) {
	// 等待 RakNet 握手完成
	select {
	case <-sess.HandshakeDone():
	case <-ctx.Done():
		_ = sess.Close()
		return
	case <-time.After(15 * time.Second):
		_ = sess.Close()
		return
	}

	res, err := detector.DetectBedrock(ctx, sess, detector.BedrockDetectOption{
		Validator:   e.validator,
		UUIDField:   e.uuidField,
		TargetField: e.targetField,
	})
	if err != nil {
		e.log.Debug("bedrock detect failed", zap.Error(err))
		_ = sess.Close()
		return
	}

	s := session.Acquire()
	defer session.Release(s)
	s.ID = e.mgr.NewID()
	s.Kind = res.Kind
	s.Proto = detector.ProtoBedrock
	if res.Login != nil {
		s.Version = res.Login.ProtocolVersion
	}
	s.RemoteAddr = sess.Remote()
	s.CreatedAt = time.Now()
	s.Touch()
	if !e.mgr.Add(s) {
		_ = sess.Close()
		return
	}
	defer e.mgr.Remove(s.ID)
	e.registry.FireSession(s, plugin.SessionOpen)
	defer e.registry.FireSession(s, plugin.SessionClose)

	switch res.Kind {
	case detector.KindTunnel:
		if err := e.tunBE.Handle(ctx, sess, res); err != nil {
			e.log.Debug("bedrock tunnel ended", zap.Error(err))
		}
	default:
		if err := e.passBE.Handle(ctx, sess, res); err != nil {
			e.log.Debug("bedrock passthrough ended", zap.Error(err))
		}
	}
}

// buildMimicMatcher 从 config.MimicConfig 构造 tunnel.MimicMatcher，填默认值。
func buildMimicMatcher(c config.MimicConfig) *tunnel.MimicMatcher {
	m := &tunnel.MimicMatcher{
		Prefix:      c.Prefix,
		MoveSuffix:  c.MoveSuffix,
		FlySuffix:   c.FlySuffix,
		ChunkSuffix: c.ChunkSuffix,
		IdleSuffix:  c.IdleSuffix,
		MoveMax:     c.MoveMax,
		FlyMax:      c.FlyMax,
		ChunkSplit:  c.ChunkSplit,
	}
	if m.Prefix == "" {
		m.Prefix = "mcte:"
	}
	if m.MoveSuffix == "" {
		m.MoveSuffix = "m"
	}
	if m.FlySuffix == "" {
		m.FlySuffix = "f"
	}
	if m.ChunkSuffix == "" {
		m.ChunkSuffix = "c"
	}
	if m.IdleSuffix == "" {
		m.IdleSuffix = "i"
	}
	if m.MoveMax <= 0 {
		m.MoveMax = 32
	}
	if m.FlyMax <= 0 {
		m.FlyMax = 256
	}
	if m.ChunkSplit <= 0 {
		m.ChunkSplit = 2048
	}
	return m
}

func itoa(p uint16) string {
	if p == 0 {
		return "0"
	}
	var buf [5]byte
	n := len(buf)
	v := p
	for v > 0 {
		n--
		buf[n] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[n:])
}
