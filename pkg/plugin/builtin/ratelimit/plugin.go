// Package ratelimit 按源 IP 限制连接速率。
//
// 抗 IP 扫描攻击：
//   - limiters 数量硬上限 maxEntries（默认 16384）；超出时 LRU 淘汰最旧
//   - GC 周期默认 30s，及时回收满桶的空闲 limiter
package ratelimit

import (
	"container/list"
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/shuffleman/mcte/pkg/plugin"
	"github.com/shuffleman/mcte/pkg/session"
)

type entry struct {
	ip      string
	limiter *rate.Limiter
	elem    *list.Element
}

type Plugin struct {
	mu       sync.Mutex
	limiters map[string]*entry
	lru      *list.List
	rps      rate.Limit
	burst    int

	maxEntries int
	gcInterval time.Duration

	gcStop chan struct{}

	dropEvict uint64
}

// New 构造插件。
func New(rps float64, burst int) *Plugin {
	return NewWithOptions(rps, burst, 16384, 30*time.Second)
}

// NewWithOptions 自定义 limiters 上限与 GC 周期。
func NewWithOptions(rps float64, burst, maxEntries int, gcInterval time.Duration) *Plugin {
	if maxEntries <= 0 {
		maxEntries = 16384
	}
	if gcInterval <= 0 {
		gcInterval = 30 * time.Second
	}
	return &Plugin{
		limiters:   make(map[string]*entry, 256),
		lru:        list.New(),
		rps:        rate.Limit(rps),
		burst:      burst,
		maxEntries: maxEntries,
		gcInterval: gcInterval,
		gcStop:     make(chan struct{}),
	}
}

func (p *Plugin) Name() string { return "ratelimit" }

func (p *Plugin) Start(ctx context.Context) error {
	go p.gcLoop(ctx)
	return nil
}

func (p *Plugin) Stop(ctx context.Context) error {
	close(p.gcStop)
	return nil
}

// Allow 由 engine 在 accept 后调用。
func (p *Plugin) Allow(ip string) bool {
	p.mu.Lock()
	e, ok := p.limiters[ip]
	if ok {
		p.lru.MoveToFront(e.elem)
		l := e.limiter
		p.mu.Unlock()
		return l.Allow()
	}
	// 容量上限：LRU 淘汰最旧
	for len(p.limiters) >= p.maxEntries {
		oldest := p.lru.Back()
		if oldest == nil {
			break
		}
		oe := oldest.Value.(*entry)
		p.lru.Remove(oldest)
		delete(p.limiters, oe.ip)
		p.dropEvict++
	}
	l := rate.NewLimiter(p.rps, p.burst)
	e = &entry{ip: ip, limiter: l}
	e.elem = p.lru.PushFront(e)
	p.limiters[ip] = e
	p.mu.Unlock()
	return l.Allow()
}

func (p *Plugin) OnSessionEvent(_ *session.Session, _ plugin.SessionEvent) {}

// Stats 监控指标。
func (p *Plugin) Stats() (size int, evicted uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.limiters), p.dropEvict
}

func (p *Plugin) gcLoop(ctx context.Context) {
	t := time.NewTicker(p.gcInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.gcStop:
			return
		case <-t.C:
			p.gcOnce()
		}
	}
}

// gcOnce 回收满桶（即没有消耗 token）的空闲 limiter。
func (p *Plugin) gcOnce() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for ip, e := range p.limiters {
		if e.limiter.Tokens() >= float64(p.burst) {
			p.lru.Remove(e.elem)
			delete(p.limiters, ip)
		}
	}
}
