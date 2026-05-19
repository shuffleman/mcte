package passthrough

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/shuffleman/mcte/pkg/detector"
	"github.com/shuffleman/mcte/pkg/protocol/bedrock/raknet"
)

// BedrockDialer 拨号到后端 Bedrock 服务器，建立 RakNet 客户端会话。
type BedrockDialer struct {
	targets []string
	timeout time.Duration
	idx     atomic.Uint32
}

func NewBedrockDialer(targets []string, timeout time.Duration) (*BedrockDialer, error) {
	if len(targets) == 0 {
		return nil, errors.New("passthrough: empty bedrock targets")
	}
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	return &BedrockDialer{targets: targets, timeout: timeout}, nil
}

// Dial 返回到后端的 RakNet 客户端 Session（已 Connected）。
func (d *BedrockDialer) Dial(ctx context.Context) (*raknet.Session, string, error) {
	n := uint32(len(d.targets))
	start := d.idx.Add(1) - 1
	var lastErr error
	for i := uint32(0); i < n; i++ {
		target := d.targets[(start+i)%n]
		dctx, cancel := context.WithTimeout(ctx, d.timeout)
		sess, err := raknet.Dial(dctx, target)
		cancel()
		if err == nil {
			return sess, target, nil
		}
		lastErr = err
	}
	return nil, "", lastErr
}

// BedrockHandler 真实 Bedrock 客户端透传到后端 Bedrock 服务器。
type BedrockHandler struct {
	dialer *BedrockDialer
}

func NewBedrockHandler(d *BedrockDialer) *BedrockHandler {
	return &BedrockHandler{dialer: d}
}

// Handle 双向桥接 RakNet 应用层 packet（含 Login 与所有后续 game packet）。
//
// detector 已读出第一个 Login game packet 字节并存放在 res.LoginRaw 中 —— 这里需要原样重放给后端，
// 然后进入对等转发循环。
func (h *BedrockHandler) Handle(ctx context.Context, client *raknet.Session, res *detector.BedrockResult) error {
	backend, _, err := h.dialer.Dial(ctx)
	if err != nil {
		_ = client.Close()
		return err
	}
	defer backend.Close()

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 重放 Login 包到后端：把 Login + tail 重新组成 game packet 发送。
	// 由于 detector 在拿到 Login 时已经解出原始 batch packets，我们用 LoginRaw 重新组 batch 发回去最简洁。
	if res.LoginRaw != nil {
		// 使用 zlib 重新压缩；后端会自行解压
		pkts := [][]byte{res.LoginRaw}
		pkts = append(pkts, res.BatchTail...)
		gp, gerr := assembleBatch(pkts)
		if gerr != nil {
			return gerr
		}
		if werr := backend.WriteApp(gp); werr != nil {
			return werr
		}
	}

	errCh := make(chan error, 2)

	// client -> backend：用 ctx 版让 cwnd 阻塞可被取消
	go func() {
		for {
			body, rerr := client.ReadApp(runCtx)
			if rerr != nil {
				errCh <- rerr
				return
			}
			if werr := backend.WriteAppCtx(runCtx, body); werr != nil {
				errCh <- werr
				return
			}
		}
	}()
	// backend -> client
	go func() {
		for {
			body, rerr := backend.ReadApp(runCtx)
			if rerr != nil {
				errCh <- rerr
				return
			}
			if werr := client.WriteAppCtx(runCtx, body); werr != nil {
				errCh <- werr
				return
			}
		}
	}()

	select {
	case e := <-errCh:
		_ = client.Close()
		return e
	case <-runCtx.Done():
		_ = client.Close()
		return runCtx.Err()
	case <-client.Closed():
		return nil
	case <-backend.Closed():
		_ = client.Close()
		return nil
	}
}
