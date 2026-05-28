package singbox

import (
	"context"
	"net"

	"go.uber.org/zap"

	"github.com/shuffleman/mcte/pkg/client"
)

type ClientOptions struct {
	Server      string `json:"server"`
	ServerPort  uint16 `json:"server_port"`
	UUID        string `json:"uuid"`
	Network     string `json:"network,omitempty"`
	Channel     string `json:"channel,omitempty"`
	UUIDField   string `json:"uuid_field,omitempty"`
	TargetField string `json:"target_field,omitempty"`

	// Mimic 启用 Java TCP 抗 DPI 流量整形（C→S 小帧 + tick-rate 移动流）。
	Mimic bool `json:"mimic,omitempty"`
	// EntropyPrefix > 0 时启用载荷熵前缀（每帧前置该字节数上限的低熵填充）。仅 Mimic 时生效。
	EntropyPrefix int `json:"entropy_prefix,omitempty"`
	// C2SRate > 0 时限制 C→S 发送速率（字节/秒）并逐帧 pacing 让小帧真上 wire。仅 Mimic 时生效。
	C2SRate int `json:"c2s_rate,omitempty"`
}

type Client struct {
	client *client.Client
}

func NewClient(opts ClientOptions, logger *zap.Logger) (*Client, error) {
	cfg := client.Config{
		Server:      opts.Server,
		Port:        opts.ServerPort,
		UUID:        opts.UUID,
		Network:     opts.Network,
		Channel:     opts.Channel,
		UUIDField:   opts.UUIDField,
		TargetField: opts.TargetField,
	}
	if opts.Mimic {
		prof := client.DefaultProfile()
		if opts.EntropyPrefix > 0 {
			prof.EntropyPrefixMax = opts.EntropyPrefix
			prof.EntropyPrefixMin = opts.EntropyPrefix / 2
		}
		if opts.C2SRate > 0 {
			prof.C2SRateBytesPerSec = opts.C2SRate
		}
		cfg.Mimic = prof
	}
	c, err := client.New(cfg)
	if err != nil {
		return nil, err
	}
	return &Client{client: c}, nil
}

func (c *Client) Dial(ctx context.Context, destHost string, destPort uint16) (net.Conn, error) {
	return c.client.Dial(ctx, destHost, destPort)
}

func (c *Client) Close() error {
	return nil
}
