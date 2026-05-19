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
