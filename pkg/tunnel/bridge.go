package tunnel

import (
	"context"
	"math/rand"
	"net"
	"strings"

	"github.com/shuffleman/mcte/pkg/protocol/java"
)

// PluginMessageMaxData Plugin Message 单帧 data 字段最大字节数。
const PluginMessageMaxData = 32767

// MimicMatcher 描述 inbound 如何识别来自客户端的"伪装多 channel"。
//
// 客户端可能用 mcte:m / mcte:f / mcte:c / mcte:i 等不同 channel 承载数据
// （来自 client.MimicProfile）。inbound 用前缀匹配统一识别为隧道流量：
//   - Prefix 匹配的：拆出 payload 转发到 upstream；
//   - IdleSuffix 命中的：丢弃 payload（空心跳）；
//   - 完全不匹配的 channel：丢弃（其他 mod 的流量）。
//
// 下行同样需要切片：上游字节按大小回填到 move/fly/chunk channel。
// 简化策略：上游每次 Read 出来的字节直接根据大小分类，按客户端同样的阈值切。
type MimicMatcher struct {
	Prefix      string
	MoveSuffix  string
	FlySuffix   string
	ChunkSuffix string
	IdleSuffix  string
	MoveMax     int // 包大小 ≤ MoveMax → 进 MoveSuffix channel
	FlyMax      int // 包大小 ≤ FlyMax → FlySuffix
	ChunkSplit  int // 大于 FlyMax 时按 ChunkSplit 切片，进 ChunkSuffix

	// FramePrefix：客户端 C→S 小帧是否携带自描述帧头 [1B prefixLen][prefixLen 字节]。
	// 与 client.MimicProfile 的小帧编码配套；为 true 时服务端剥离帧头再转发上游。
	// 新客户端恒为 true（即使 prefixLen=0 也带 1 字节头）。
	FramePrefix bool
}

// DefaultMimicMatcher 与 client.DefaultProfile 一致的默认值。
func DefaultMimicMatcher() *MimicMatcher {
	return &MimicMatcher{
		Prefix:      "mcte:",
		MoveSuffix:  "m",
		FlySuffix:   "f",
		ChunkSuffix: "c",
		IdleSuffix:  "i",
		MoveMax:     32,
		FlyMax:      256,
		ChunkSplit:  2048,
		FramePrefix: true,
	}
}

// stripFramePrefix 剥离 C→S 小帧自描述帧头 [1B prefixLen][prefixLen 字节]，返回真实数据。
// 格式异常（头声明长度超出 buffer）时返回 nil，调用方应丢弃该帧。
func stripFramePrefix(d []byte) []byte {
	if len(d) == 0 {
		return nil
	}
	l := int(d[0])
	if 1+l > len(d) {
		return nil
	}
	return d[1+l:]
}

// Bridge 在客户端 Play 阶段 PluginMessage 与上游 net.Conn 之间双向桥接。
type Bridge struct {
	fr        *java.Framer
	proto     int32
	channel   string // 兼容模式单 channel；matcher 非空时忽略
	matcher   *MimicMatcher
	client    net.Conn
	upstream  net.Conn
	keepalive *KeepAliveDriver
}

// NewBridge 构造兼容模式 bridge（单 channel）。
func NewBridge(fr *java.Framer, proto int32, channel string, client, upstream net.Conn, ka *KeepAliveDriver) *Bridge {
	return &Bridge{fr: fr, proto: proto, channel: channel, client: client, upstream: upstream, keepalive: ka}
}

// NewBridgeWithMimic 构造带流量画像的 bridge。
// matcher 非 nil 时按 prefix 识别所有伪装 channel，下行也用 matcher 切片。
func NewBridgeWithMimic(fr *java.Framer, proto int32, matcher *MimicMatcher, client, upstream net.Conn, ka *KeepAliveDriver) *Bridge {
	return &Bridge{fr: fr, proto: proto, matcher: matcher, client: client, upstream: upstream, keepalive: ka}
}

// Run 启动桥接。
func (b *Bridge) Run(ctx context.Context) error {
	cbID := java.PacketID(b.proto, java.StatePlay, java.ClientBound, java.PPlayPluginMessageCB)
	sbKeepAliveID := java.PacketID(b.proto, java.StatePlay, java.ServerBound, java.PKeepAliveSB)
	sbPluginID := java.PacketID(b.proto, java.StatePlay, java.ServerBound, java.PPlayPluginMessageSB)

	errCh := make(chan error, 2)

	// downstream: 客户端 → 上游
	go func() {
		for {
			pkt, err := b.fr.ReadPacket()
			if err != nil {
				errCh <- err
				return
			}
			switch pkt.ID {
			case sbKeepAliveID:
				if b.keepalive != nil {
					b.keepalive.MarkRecv()
				}
			case sbPluginID:
				pm, err := java.DecodePluginMessage(pkt.Payload)
				if err != nil {
					errCh <- err
					return
				}
				if !b.channelAccepted(pm.Channel) {
					continue
				}
				if b.isIdleChannel(pm.Channel) {
					continue
				}
				data := pm.Data
				// C→S 小帧剥离自描述帧头（[1B prefixLen][prefixLen 低熵字节]）拼回原始流。
				if b.matcher != nil && b.matcher.FramePrefix {
					data = stripFramePrefix(data)
				}
				if len(data) == 0 {
					continue
				}
				if _, err := b.upstream.Write(data); err != nil {
					errCh <- err
					return
				}
			}
			if ctx.Err() != nil {
				errCh <- ctx.Err()
				return
			}
		}
	}()

	// upstream: 上游 → 客户端
	go func() {
		buf := make([]byte, PluginMessageMaxData)
		for {
			n, err := b.upstream.Read(buf)
			if n > 0 {
				chunk := make([]byte, n)
				copy(chunk, buf[:n])
				if werr := b.writeDownstream(cbID, chunk); werr != nil {
					errCh <- werr
					return
				}
			}
			if err != nil {
				errCh <- err
				return
			}
			if ctx.Err() != nil {
				errCh <- ctx.Err()
				return
			}
		}
	}()

	select {
	case e := <-errCh:
		b.closeBoth()
		return e
	case <-ctx.Done():
		b.closeBoth()
		return ctx.Err()
	}
}

// channelAccepted 判断 PluginMessage channel 是否属于本隧道流量。
func (b *Bridge) channelAccepted(ch string) bool {
	if b.matcher != nil {
		return strings.HasPrefix(ch, b.matcher.Prefix)
	}
	return ch == b.channel
}

// isIdleChannel 判断是否是心跳 channel。
func (b *Bridge) isIdleChannel(ch string) bool {
	if b.matcher == nil {
		return false
	}
	return ch == b.matcher.Prefix+b.matcher.IdleSuffix
}

// writeDownstream 把上游字节回写到客户端 Plugin Message。
// matcher 非 nil 时按大小切片到 move/fly/chunk channel；否则用单 channel + 32767 分帧。
func (b *Bridge) writeDownstream(cbID int32, data []byte) error {
	if b.matcher == nil {
		pm := &java.PluginMessage{Channel: b.channel, Data: data}
		return b.fr.WritePacket(java.Packet{ID: cbID, Payload: java.EncodePluginMessage(pm)})
	}
	m := b.matcher
	for len(data) > 0 {
		var (
			take    int
			channel string
		)
		switch {
		case len(data) <= m.MoveMax:
			take = len(data)
			channel = m.Prefix + m.MoveSuffix
		case len(data) <= m.FlyMax:
			take = len(data)
			channel = m.Prefix + m.FlySuffix
		default:
			take = m.ChunkSplit
			if take > len(data) {
				take = len(data)
			}
			channel = m.Prefix + m.ChunkSuffix
		}
		pm := &java.PluginMessage{Channel: channel, Data: data[:take]}
		if err := b.fr.WritePacket(java.Packet{ID: cbID, Payload: java.EncodePluginMessage(pm)}); err != nil {
			return err
		}
		data = data[take:]
	}
	return nil
}

// closeBoth 同时关上下游；幂等。
func (b *Bridge) closeBoth() {
	_ = b.upstream.Close()
	if b.client != nil {
		_ = b.client.Close()
	}
}

// jitter 给一个 [0, ±max] 范围内的扰动（保留 API，未来用于错乱包大小分布抗指纹）。
//
//nolint:unused
func jitter(max int) int {
	if max <= 0 {
		return 0
	}
	return rand.Intn(2*max+1) - max
}
