// Package scheduler 提供 20TPS 调度与发送整形。
package scheduler

import (
	"context"
	"time"
)

// Ticker 20TPS 滑动窗口补偿型 Ticker。
type Ticker struct {
	tps      int
	interval time.Duration
}

func NewTicker(tps int) *Ticker {
	if tps <= 0 {
		tps = 20
	}
	return &Ticker{tps: tps, interval: time.Second / time.Duration(tps)}
}

// Run 调用 fn(tickNo, elapsed)；ctx 取消时退出。
func (t *Ticker) Run(ctx context.Context, fn func(tick uint64, dt time.Duration)) {
	tk := time.NewTicker(t.interval)
	defer tk.Stop()
	var (
		count uint64
		last  = time.Now()
	)
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-tk.C:
			dt := now.Sub(last)
			last = now
			count++
			fn(count, dt)
		}
	}
}
