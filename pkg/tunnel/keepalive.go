package tunnel

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/shuffleman/mcte/pkg/protocol/java"
)

// KeepAliveDriver 在独立 goroutine 中驱动 KeepAlive，与 bridge 并行。
type KeepAliveDriver struct {
	fr       *java.Framer
	proto    int32
	interval time.Duration
	timeout  time.Duration

	lastRecv atomic.Int64 // 最近一次客户端回包的时间戳
}

func NewKeepAliveDriver(fr *java.Framer, proto int32, interval, timeout time.Duration) *KeepAliveDriver {
	if interval == 0 {
		interval = 20 * time.Second
	}
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	k := &KeepAliveDriver{fr: fr, proto: proto, interval: interval, timeout: timeout}
	k.lastRecv.Store(time.Now().UnixNano())
	return k
}

// MarkRecv 上层在收到 KeepAlive 客户端响应时调用。
func (k *KeepAliveDriver) MarkRecv() { k.lastRecv.Store(time.Now().UnixNano()) }

// Run 在独立 goroutine 中调用。错误（超时）会通过 ctx 取消传播。
func (k *KeepAliveDriver) Run(ctx context.Context, cancel context.CancelFunc) {
	id := java.PacketID(k.proto, java.StatePlay, java.ClientBound, java.PKeepAliveCB)
	tick := time.NewTicker(k.interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case t := <-tick.C:
			payload := java.EncodeKeepAlive(&java.KeepAlive{ID: t.UnixNano()})
			if err := k.fr.WritePacket(java.Packet{ID: id, Payload: payload}); err != nil {
				cancel()
				return
			}
			if time.Since(time.Unix(0, k.lastRecv.Load())) > k.timeout {
				cancel()
				return
			}
		}
	}
}
