package tunnel

import (
	"context"
	"time"

	"github.com/shuffleman/mcte/internal/netutil"
	"github.com/shuffleman/mcte/pkg/detector"
	"github.com/shuffleman/mcte/pkg/protocol/bedrock"
	"github.com/shuffleman/mcte/pkg/protocol/bedrock/raknet"
)

// BedrockHandler 实现 Bedrock 隧道流程。
//
// 协议约定：客户端与 MCTE 之间通过自定义 packet ID `bedrock.PacketTunnelData` (0x200)
// 承载隧道字节，header 之后即为 payload。其他 packet ID（如 PlayStatus 等）由 MCTE
// 本端响应或直接丢弃，不会泄露到上游。
type BedrockHandler struct {
	cfg  HandlerConfig
	dial UpstreamDialFunc
}

func NewBedrockHandler(cfg HandlerConfig, dial UpstreamDialFunc) *BedrockHandler {
	if cfg.KeepAliveEvery == 0 {
		cfg.KeepAliveEvery = 5 * time.Second
	}
	if cfg.KeepAliveTimeout == 0 {
		cfg.KeepAliveTimeout = 30 * time.Second
	}
	return &BedrockHandler{cfg: cfg, dial: dial}
}

// Handle 处理已识别为隧道的 Bedrock 会话。
func (h *BedrockHandler) Handle(ctx context.Context, sess *raknet.Session, res *detector.BedrockResult) error {
	// 发 PlayStatus(LoginSuccess) 让客户端进入"已登入"状态。
	payload := bedrock.EncodePlayStatusPayload(bedrock.PlayStatusLoginSuccess)
	gp, err := bedrock.AssembleSingle(bedrock.CompZlib, bedrock.PacketPlayStatus, payload)
	if err != nil {
		return err
	}
	if err := sess.WriteApp(gp); err != nil {
		return err
	}

	// 拨号上游
	upCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	rawUpstream, err := h.dial(upCtx, res.Target, res.Port)
	cancel()
	if err != nil {
		_ = sess.Close()
		return err
	}
	// upstream 半开检测：30s 内任一 Write 阻塞超时则强 Close → upstream.Read/Write 返回
	// → errCh 触发 → sess.Close → 上下游断开事件双向传递。
	wdTimeout := 30 * time.Second
	if h.cfg.KeepAliveTimeout > 0 {
		wdTimeout = h.cfg.KeepAliveTimeout
	}
	upstream := netutil.NewWatchdogConn(rawUpstream, wdTimeout, 0)
	upstream.Start(ctx)
	defer upstream.Close()

	runCtx, cancel2 := context.WithCancel(ctx)
	defer cancel2()

	// 批包尾部若已携带 TunnelData，提前转发给上游
	for _, pkt := range res.BatchTail {
		if data, ok := extractTunnelPayload(pkt); ok {
			if _, werr := upstream.Write(data); werr != nil {
				return werr
			}
		}
	}

	errCh := make(chan error, 2)

	// 上行：client → upstream
	go func() {
		for {
			body, err := sess.ReadApp(runCtx)
			if err != nil {
				errCh <- err
				return
			}
			if len(body) == 0 || body[0] != bedrock.IDGamePacket {
				continue
			}
			decompressed, err := bedrock.DecompressBatch(body[1:])
			if err != nil {
				errCh <- err
				return
			}
			pkts, err := bedrock.SplitBatchPackets(decompressed)
			if err != nil {
				errCh <- err
				return
			}
			for _, p := range pkts {
				if data, ok := extractTunnelPayload(p); ok {
					if _, werr := upstream.Write(data); werr != nil {
						errCh <- werr
						return
					}
				}
				// 非 TunnelData 的 packet 全部静默丢弃
			}
		}
	}()

	// 下行：upstream → client，每个 Read 块包装成一个 TunnelData packet
	go func() {
		buf := make([]byte, 8*1024)
		for {
			n, err := upstream.Read(buf)
			if n > 0 {
				chunk := make([]byte, n)
				copy(chunk, buf[:n])
				gp, gerr := bedrock.AssembleSingle(bedrock.CompZlib, bedrock.PacketTunnelData, chunk)
				if gerr != nil {
					errCh <- gerr
					return
				}
				if werr := sess.WriteAppCtx(runCtx, gp); werr != nil {
					errCh <- werr
					return
				}
			}
			if err != nil {
				errCh <- err
				return
			}
		}
	}()

	select {
	case e := <-errCh:
		_ = sess.Close()
		return e
	case <-runCtx.Done():
		_ = sess.Close()
		return runCtx.Err()
	case <-sess.Closed():
		return nil
	}
}

// extractTunnelPayload 检查 packet 是否为 PacketTunnelData，是则返回 payload。
func extractTunnelPayload(pkt []byte) ([]byte, bool) {
	hdr, n, err := bedrock.ReadVarUint32(pkt)
	if err != nil {
		return nil, false
	}
	if hdr&0x3FF != bedrock.PacketTunnelData {
		return nil, false
	}
	return pkt[n:], true
}
