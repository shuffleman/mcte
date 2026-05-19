package scheduler

import (
	"context"
	"time"

	"golang.org/x/time/rate"
)

// Shaper 包裹 token bucket，按字节计量。
type Shaper struct {
	lim *rate.Limiter
}

func NewShaper(bytesPerSec, burst int) *Shaper {
	if bytesPerSec <= 0 {
		return &Shaper{lim: rate.NewLimiter(rate.Inf, 0)}
	}
	if burst <= 0 {
		burst = bytesPerSec
	}
	return &Shaper{lim: rate.NewLimiter(rate.Limit(bytesPerSec), burst)}
}

// Wait 阻塞直到允许发送 n 字节。
func (s *Shaper) Wait(ctx context.Context, n int) error {
	return s.lim.WaitN(ctx, n)
}

// Allow 非阻塞判断。
func (s *Shaper) Allow(n int) bool { return s.lim.AllowN(time.Now(), n) }
