package singbox

import (
	"context"
	"io"
	"net"

	"go.uber.org/zap"

	"github.com/shuffleman/mcte/pkg/client"
)

// Outbound MCTE 作为 sing-box outbound 的实现。
//
// 在 sing-box fork 中注册：
//   adapter.RegisterOutbound("minecraft", func(ctx, router, logger, tag, opts) (adapter.Outbound, error) {
//       return mcteSingbox.NewOutbound(opts, logger)
//   })
//
// 实际 adapter.Outbound 还需要实现 DialContext / ListenPacket / Network / Tag 等接口，
// 这里给出协议核心，由调用方在 fork 内套用 sing-box 的 adapter.Adapter 模板。
type Outbound struct {
	sdk *client.Client
	log *zap.Logger
}

// NewOutbound 构造。
func NewOutbound(opts OutboundOptions, log *zap.Logger) (*Outbound, error) {
	if log == nil {
		log = zap.NewNop()
	}
	network := opts.Network
	if network == "" {
		network = "tcp"
	}
	sdk, err := client.New(client.Config{
		Server:      opts.Server,
		Port:        opts.ServerPort,
		UUID:        opts.UUID,
		Network:     network,
		Channel:     opts.Channel,
		UUIDField:   opts.UUIDField,
		TargetField: opts.TargetField,
	})
	if err != nil {
		return nil, err
	}
	return &Outbound{sdk: sdk, log: log}, nil
}

// DialContext 用 MCTE client SDK 拨号到远端 inbound。
// destination 由调用方在 sing-box adapter 层提供（解 Socksaddr → host/port）。
func (o *Outbound) DialContext(ctx context.Context, host string, port uint16) (net.Conn, error) {
	return o.sdk.Dial(ctx, host, port)
}

// Bridge 接收 sing-box 路由层产生的 src conn（来自用户），用 MCTE 拨号目标后双向桥接。
func (o *Outbound) Bridge(ctx context.Context, src net.Conn, host string, port uint16) error {
	dst, err := o.sdk.Dial(ctx, host, port)
	if err != nil {
		return err
	}
	defer dst.Close()
	errCh := make(chan error, 2)
	go func() { _, e := io.Copy(dst, src); errCh <- e }()
	go func() { _, e := io.Copy(src, dst); errCh <- e }()
	select {
	case <-errCh:
	case <-ctx.Done():
	}
	return nil
}
