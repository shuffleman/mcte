package netutil

import (
	"sync"
	"time"
)

// TokenBucket 字节级令牌桶，sleep-based pacing。
//
// 用途：抗 DPI 流量整形。把发送速率限到接近真实 MC 游戏流量级别，压制
// seq_delta_rate（吞吐速率）等监督模型头号特征；同时小 burst + 逐帧 Take
// 可让应用层小帧在 NODELAY 的 TCP 上**真正以小包上 wire**（帧间有时间间隔，
// 内核不会 coalesce 成 MSS 大 segment）。
//
// 并发：假设每个方向只有单一写者顺序调用 Take（client writeMimic / server
// writeDownstream 均为单 goroutine 串行写），内部仍加锁以防意外并发。
// rate <= 0 表示不限速，Take 立即返回（零开销）。
type TokenBucket struct {
	mu     sync.Mutex
	rate   float64 // bytes/sec
	burst  float64 // 桶容量（决定瞬时突发量；越小包越分散）
	tokens float64
	last   time.Time
}

// NewTokenBucket 构造令牌桶。rate/burst 单位字节、字节/秒。rate<=0 = 不限速。
func NewTokenBucket(rate, burst int) *TokenBucket {
	return &TokenBucket{
		rate:   float64(rate),
		burst:  float64(burst),
		tokens: float64(burst),
		last:   time.Now(),
	}
}

// Take 消费 n 字节令牌；不足时 sleep 补齐（pacing）。rate<=0 直接返回。
func (b *TokenBucket) Take(n int) {
	if b.rate <= 0 {
		return
	}
	b.mu.Lock()
	now := time.Now()
	b.tokens += b.rate * now.Sub(b.last).Seconds()
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
	b.last = now
	b.tokens -= float64(n)
	var wait time.Duration
	if b.tokens < 0 {
		wait = time.Duration(-b.tokens / b.rate * float64(time.Second))
	}
	b.mu.Unlock()
	if wait > 0 {
		time.Sleep(wait)
	}
}

// Enabled 是否启用限速。
func (b *TokenBucket) Enabled() bool { return b != nil && b.rate > 0 }
