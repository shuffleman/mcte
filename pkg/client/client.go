// Package client 提供 MCTE outbound 客户端 SDK。
//
// 用法：
//   sdk := client.New(client.Config{
//       Server:      "mcte.example.com",
//       Port:        25565,
//       UUID:        "550e8400-e29b-41d4-a716-446655440000",
//       Network:     "tcp", // 或 "udp"（Bedrock）
//   })
//   conn, err := sdk.Dial(ctx, "target.host.com", 8080)
//   // 拿到的 conn 是一个 net.Conn，读写即可，对方应用层会收到字节流
package client

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/google/uuid"

	"github.com/shuffleman/mcte/pkg/detector"
	"github.com/shuffleman/mcte/pkg/protocol/java"
)

// Config 客户端配置。
type Config struct {
	Server      string // 远端 MCTE inbound 主机
	Port        uint16 // 端口（Java 25565 / Bedrock 19132）
	UUID        string // 身份 UUID
	Network     string // tcp / udp，默认 tcp
	Channel     string // 兼容旧字段：未设 Mimic 时使用此单一 channel；默认 "mcte:data"
	UUIDField   string // Bedrock JWT claim 字段，默认 MCTEUuid
	TargetField string // 同上，默认 MCTETarget
	DialTimeout time.Duration
	// PretendHost 客户端在 handshake 中伪装的 host 部分。
	// 例如填 "minecraft.example.com"，整个 serverAddress 就变成
	// "minecraft.example.com\x00<uuid>"，看起来像普通 MC 客户端发的字段。
	PretendHost string
	// PretendVersion 假装的协议版本号（不影响功能，只为流量画像）。默认 769 (1.21.4)。
	PretendVersion int32
	// Mimic 流量画像：按数据大小映射到不同 channel（move/fly/chunk/idle）。
	// 设为 nil 时使用单一 Channel（兼容模式）。
	// 推荐 DefaultProfile()，inbound 默认 prefix "mcte:" 配套识别。
	Mimic *MimicProfile
}

// Client outbound 客户端。
type Client struct {
	cfg Config
	id  uuid.UUID
}

// New 构造客户端。
func New(cfg Config) (*Client, error) {
	if cfg.Server == "" || cfg.Port == 0 {
		return nil, errors.New("client: server/port required")
	}
	if cfg.UUID == "" {
		return nil, errors.New("client: uuid required")
	}
	id, err := uuid.Parse(cfg.UUID)
	if err != nil {
		return nil, err
	}
	if cfg.Network == "" {
		cfg.Network = "tcp"
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 10 * time.Second
	}
	if cfg.Channel == "" {
		cfg.Channel = "mcte:data"
	}
	if cfg.UUIDField == "" {
		cfg.UUIDField = "MCTEUuid"
	}
	if cfg.TargetField == "" {
		cfg.TargetField = "MCTETarget"
	}
	if cfg.PretendHost == "" {
		cfg.PretendHost = cfg.Server
	}
	if cfg.PretendVersion == 0 {
		cfg.PretendVersion = java.ProtocolV1_21_4
	}
	return &Client{cfg: cfg, id: id}, nil
}

// Network 返回当前客户端的协议（tcp / udp）。
func (c *Client) Network() string { return c.cfg.Network }

// Dial 拨号一条到 MCTE inbound 的 conn，目标地址 destHost:destPort 由 inbound 解析后路由出去。
// 返回的 net.Conn 是应用层透明字节流。
//
// 实现细节：Java 路径在握手时把 UUID 通过 serverAddress 字段（与 BungeeCord 兼容）传给 inbound；
// Bedrock 路径在 LoginPacket.clientData JWT claim 中带 UUID 与目标地址。
func (c *Client) Dial(ctx context.Context, destHost string, destPort uint16) (net.Conn, error) {
	switch c.cfg.Network {
	case "tcp", "":
		return c.dialJava(ctx, destHost, destPort)
	case "udp":
		return c.dialBedrock(ctx, destHost, destPort)
	}
	return nil, errors.New("client: unknown network " + c.cfg.Network)
}

// dialJava 完成 Java MC 隧道握手并返回桥接的 net.Conn。
func (c *Client) dialJava(ctx context.Context, destHost string, destPort uint16) (net.Conn, error) {
	addr := net.JoinHostPort(c.cfg.Server, itoa(c.cfg.Port))
	d := &net.Dialer{KeepAlive: 30 * time.Second}
	dctx, cancel := context.WithTimeout(ctx, c.cfg.DialTimeout)
	defer cancel()
	conn, err := d.DialContext(dctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	fr := java.NewFramer(conn)

	// Handshake: nextState=2 (Login)
	hs := &java.Handshake{
		ProtocolVersion: c.cfg.PretendVersion,
		ServerAddress:   detector.EncodeAddrWithUUID(c.cfg.PretendHost, c.id),
		ServerPort:      c.cfg.Port,
		NextState:       2,
	}
	if err := fr.WritePacket(java.Packet{ID: 0x00, Payload: java.EncodeHandshake(hs)}); err != nil {
		_ = conn.Close()
		return nil, err
	}

	// LoginStart
	ls := &java.LoginStart{Username: "mcte-client", UUID: [16]byte(c.id)}
	loginStartID := java.PacketID(c.cfg.PretendVersion, java.StateLogin, java.ServerBound, java.PLoginStart)
	if err := fr.WritePacket(java.Packet{ID: loginStartID, Payload: java.EncodeLoginStart(ls)}); err != nil {
		_ = conn.Close()
		return nil, err
	}

	// 等 LoginPluginRequest，回 LoginPluginResponse 携带目标
	respID := java.PacketID(c.cfg.PretendVersion, java.StateLogin, java.ServerBound, java.PLoginPluginResponse)
	for {
		pkt, err := fr.ReadPacket()
		if err != nil {
			_ = conn.Close()
			return nil, err
		}
		if pkt.ID != java.PacketID(c.cfg.PretendVersion, java.StateLogin, java.ClientBound, java.PLoginPluginRequest) {
			// 跳过其他 server-bound 消息（如 SetCompression）
			continue
		}
		// 解析 messageID
		r := java.NewReader(pkt.Payload)
		msgID, err := r.ReadVarInt()
		if err != nil {
			_ = conn.Close()
			return nil, err
		}
		// 构造目标 data：1B ver + 2B hostLen + host + 2B port
		data := encodeNegotiateData(destHost, destPort)
		resp := &java.LoginPluginResponse{
			MessageID:  msgID,
			Successful: true,
			Data:       data,
		}
		payload := encodeLoginPluginResponse(resp)
		if err := fr.WritePacket(java.Packet{ID: respID, Payload: payload}); err != nil {
			_ = conn.Close()
			return nil, err
		}
		break
	}

	// 此后 conn 上承载的是 MC Play 状态的 Plugin Message 流。
	// 为了简化对外接口，我们用一个适配器把 Plugin Message 拆装屏蔽。
	bridge := newJavaBridgeConn(fr, conn, c.cfg.Channel, c.cfg.PretendVersion, c.cfg.Mimic)
	if c.cfg.Mimic != nil && c.cfg.Mimic.HeartbeatInterval > 0 {
		bridge.startHeartbeat()
	}
	return bridge, nil
}

func encodeNegotiateData(host string, port uint16) []byte {
	out := make([]byte, 0, 5+len(host))
	out = append(out, 1) // version=1
	out = append(out, byte(len(host)>>8), byte(len(host)))
	out = append(out, host...)
	out = append(out, byte(port>>8), byte(port))
	return out
}

func encodeLoginPluginResponse(p *java.LoginPluginResponse) []byte {
	w := java.NewWriterCap(8 + len(p.Data))
	w.WriteVarInt(p.MessageID)
	w.WriteBool(p.Successful)
	if p.Successful {
		w.WriteRaw(p.Data)
	}
	return w.Bytes()
}

func itoa(v uint16) string {
	if v == 0 {
		return "0"
	}
	var buf [5]byte
	n := len(buf)
	for v > 0 {
		n--
		buf[n] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[n:])
}
