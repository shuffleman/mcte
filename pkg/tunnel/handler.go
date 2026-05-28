package tunnel

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/google/uuid"

	"github.com/shuffleman/mcte/internal/netutil"
	"github.com/shuffleman/mcte/pkg/detector"
	"github.com/shuffleman/mcte/pkg/protocol/java"
)

// UpstreamDialFunc 由调用方注入：根据协商出的 host:port 拨号上游 net.Conn。
type UpstreamDialFunc func(ctx context.Context, host string, port uint16) (net.Conn, error)

// HandlerConfig 隧道 handler 配置。
type HandlerConfig struct {
	NegotiateChannel string
	DataChannel      string
	KeepAliveEvery   time.Duration
	KeepAliveTimeout time.Duration
	// WriteTimeout 客户端 TCP 写超时；超时即认定客户端死亡并关闭连接。
	// 设 0 默认 30s。设为负数禁用（不推荐）。
	WriteTimeout time.Duration

	// Mimic 流量画像匹配器（与客户端 MimicProfile 配套）。
	// 非 nil 时使用前缀匹配识别多个 channel（mcte:m/mcte:f/mcte:c/mcte:i），
	// nil 时使用 DataChannel 单 channel（兼容模式）。
	Mimic *MimicMatcher

	// S2CRateBytesPerSec：下行（server→client）发送速率上限（字节/秒，0 = 不限速）。
	// 抗 DPI：压制 seq_delta_rate（吞吐速率）。代价是限下载速度。
	S2CRateBytesPerSec int
}

// Handler 隧道入口。
type Handler struct {
	cfg  HandlerConfig
	dial UpstreamDialFunc
}

func NewHandler(cfg HandlerConfig, dial UpstreamDialFunc) *Handler {
	if cfg.NegotiateChannel == "" {
		cfg.NegotiateChannel = NegotiateChannel
	}
	if cfg.DataChannel == "" {
		cfg.DataChannel = "mcte:data"
	}
	if cfg.KeepAliveEvery == 0 {
		cfg.KeepAliveEvery = 20 * time.Second
	}
	if cfg.KeepAliveTimeout == 0 {
		cfg.KeepAliveTimeout = 30 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 30 * time.Second
	}
	return &Handler{cfg: cfg, dial: dial}
}

// Handle 完整隧道流程：
//   1. 已识别为 Tunnel 的 RestoredConn，握手字节会被读出后立即重新进入 Login。
//   2. Login Plugin Request/Response 协商上游 host:port。
//   3. 发送虚拟 Play 状态包。
//   4. 拨号上游。
//   5. 启动 bridge + keepalive。
func (h *Handler) Handle(ctx context.Context, client net.Conn, det *detector.DetectResult) error {
	if det == nil || det.Handshake == nil {
		_ = client.Close()
		return errors.New("tunnel: missing detect result")
	}

	// 用 WatchdogConn 包装：后台 goroutine 监测 Write 阻塞时长，超过 WriteTimeout
	// 强关 conn，所有读写返回 net.ErrClosed。与 TimeoutConn 不同，它不依赖
	// SetWriteDeadline 的逐次重置（多 writer 共享时会被新 Write 不断推后失效），
	// 而是只跟踪"最早一次未完成的 Write 开始时间"。
	watchdog := netutil.NewWatchdogConn(client, h.cfg.WriteTimeout, 0)
	watchdog.Start(ctx)
	defer watchdog.Stop()
	fr := java.NewFramer(watchdog)

	// 读 Handshake（已被 RestoredConn 重放）
	if _, err := fr.ReadPacket(); err != nil {
		_ = client.Close()
		return err
	}

	// 读 LoginStart
	pkt, err := fr.ReadPacket()
	if err != nil {
		_ = client.Close()
		return err
	}
	ls, err := java.DecodeLoginStart(pkt.Payload)
	if err != nil {
		_ = client.Close()
		return err
	}
	if ls.UUID == ([16]byte{}) {
		ls.UUID = uuid.New()
	}

	// 发 LoginPluginRequest（协商）
	proto := det.Handshake.ProtocolVersion
	negID := java.PacketID(proto, java.StateLogin, java.ClientBound, java.PLoginPluginRequest)
	reqMsgID := int32(1)
	req := BuildLoginPluginRequest(reqMsgID, h.cfg.NegotiateChannel)
	if err := fr.WritePacket(java.Packet{ID: negID, Payload: java.EncodeLoginPluginRequest(req)}); err != nil {
		_ = client.Close()
		return err
	}

	// 读 LoginPluginResponse
	respID := java.PacketID(proto, java.StateLogin, java.ServerBound, java.PLoginPluginResponse)
	pkt, err = fr.ReadPacket()
	if err != nil {
		_ = client.Close()
		return err
	}
	if pkt.ID != respID {
		_ = client.Close()
		return errors.New("tunnel: unexpected packet during negotiate")
	}
	resp, err := java.DecodeLoginPluginResponse(pkt.Payload)
	if err != nil || !resp.Successful {
		_ = client.Close()
		return errors.New("tunnel: negotiate response invalid")
	}
	meta, err := DecodeNegotiateResponse(resp.Data)
	if err != nil {
		_ = client.Close()
		return err
	}

	// 进入 Play 状态
	if err := EnterPlayState(fr, proto, ls.Username, ls.UUID); err != nil {
		_ = client.Close()
		return err
	}

	// 拨号上游
	upCtx, upCancel := context.WithTimeout(ctx, 10*time.Second)
	rawUpstream, err := h.dial(upCtx, meta.Host, meta.Port)
	upCancel()
	if err != nil {
		_ = client.Close()
		return err
	}
	// upstream 同样用 WatchdogConn 包，半开 30s 内强关；upstream 断开会让 bridge 出错
	// → 触发 client.Close 让上下游断开事件双向传递。
	upstream := netutil.NewWatchdogConn(rawUpstream, h.cfg.WriteTimeout, 0)
	upstream.Start(ctx)
	defer upstream.Stop()

	// 启动 keepalive + bridge
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	ka := NewKeepAliveDriver(fr, proto, h.cfg.KeepAliveEvery, h.cfg.KeepAliveTimeout)
	go ka.Run(runCtx, cancel)

	var br *Bridge
	if h.cfg.Mimic != nil {
		br = NewBridgeWithMimic(fr, proto, h.cfg.Mimic, watchdog, upstream, ka)
	} else {
		br = NewBridge(fr, proto, h.cfg.DataChannel, watchdog, upstream, ka)
	}
	br.SetS2CRate(h.cfg.S2CRateBytesPerSec)
	err = br.Run(runCtx)
	// bridge.closeBoth 已经关了 client 与 upstream；这里 close 是幂等保险
	_ = client.Close()
	return err
}
