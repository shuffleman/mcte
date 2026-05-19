// Package xray 提供 MCTE 在 Xray 中作为 inbound/outbound 的胶水库。
//
// 设计原则：不依赖 Xray protobuf，使用方在自己的 Xray fork 里写一个薄 wrapper
// 完成 init 注册 + JSON config 解析即可。
//
// inbound 模型：MCTE 在自己进程内监听 25565/19132，协商出目标后通过 Dispatcher 投递给
// Xray 的路由层；不需要 Xray 的 transport listener。
//
// outbound 模型：使用 MCTE client SDK 拨号到远端 MCTE inbound，把 link.Reader/Writer
// 与拨号出来的 net.Conn 透传。
package xray

import (
	"context"
	"io"
	"net"

	"go.uber.org/zap"

	"github.com/shuffleman/mcte/pkg/client"
)

// Dispatcher 抽象 Xray 路由层（避免 import xray-core）。
// 实际接入时填 routing.Dispatcher.Dispatch 的适配。
type Dispatcher interface {
	Dispatch(ctx context.Context, conn net.Conn, host string, port uint16) error
}

// Server inbound 入口。已包含一个完整的 MCTE engine，自己监听端口。
type Server = Listener // 使用已有 Listener；新增工厂方法

// NewServer 推荐入口，签名更直观。
func NewServer(cfg MCTEConfig, log *zap.Logger, dispatcher Dispatcher) (*Server, error) {
	return NewListener(cfg, log, func(c net.Conn) {
		// dispatcher.Dispatch 期望接收已识别目标的 conn，但这里 c 是 net.Pipe 端，
		// host/port 通过 taggedConn 暴露。
		if t, ok := c.(taggedConn); ok {
			_ = dispatcher.Dispatch(context.Background(), c, t.host, t.port)
		} else {
			_ = c.Close()
		}
	})
}

// Client outbound 入口。
type Client struct {
	cfg MCTEClientConfig
	sdk *client.Client
	log *zap.Logger
}

// NewClient 构造 outbound。
func NewClient(cfg MCTEClientConfig, log *zap.Logger) (*Client, error) {
	if log == nil {
		log = zap.NewNop()
	}
	network := cfg.Network
	if network == "" {
		network = "tcp"
	}
	sdk, err := client.New(client.Config{
		Server:      cfg.Server,
		Port:        cfg.ServerPort,
		UUID:        cfg.UUID,
		Network:     network,
		Channel:     cfg.Channel,
		UUIDField:   cfg.UUIDField,
		TargetField: cfg.TargetField,
	})
	if err != nil {
		return nil, err
	}
	return &Client{cfg: cfg, sdk: sdk, log: log}, nil
}

// Dial 拨号到远端 MCTE inbound，返回 net.Conn。
func (c *Client) Dial(ctx context.Context, host string, port uint16) (net.Conn, error) {
	return c.sdk.Dial(ctx, host, port)
}

// Process 接收 Xray 的 link（Reader+Writer）+ 目标 host/port，
// 用 MCTE client SDK 拨号到远端 inbound 并双向透传。
//
// 调用方应在 Xray Outbound.Process 中调用本函数：
//
//	ob := session.OutboundFromContext(ctx)
//	return client.Process(ctx, link, ob.Target.Address.Domain(), uint16(ob.Target.Port))
func (c *Client) Process(ctx context.Context, reader io.Reader, writer io.Writer, host string, port uint16) error {
	conn, err := c.sdk.Dial(ctx, host, port)
	if err != nil {
		return err
	}
	defer conn.Close()

	errCh := make(chan error, 2)
	go func() {
		_, e := io.Copy(conn, reader)
		errCh <- e
	}()
	go func() {
		_, e := io.Copy(writer, conn)
		errCh <- e
	}()
	select {
	case <-errCh:
	case <-ctx.Done():
	}
	return nil
}
