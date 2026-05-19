package plugin

import (
	"context"
	"sync"

	"github.com/shuffleman/mcte/pkg/protocol/java"
	"github.com/shuffleman/mcte/pkg/session"
)

// Registry 维护已注册的插件与钩子。
type Registry struct {
	mu        sync.RWMutex
	plugins   []Plugin
	pktHooks  []PacketHook
	sessHooks []SessionHook
}

func NewRegistry() *Registry { return &Registry{} }

func (r *Registry) Register(p Plugin) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.plugins = append(r.plugins, p)
	if h, ok := p.(PacketHook); ok {
		r.pktHooks = append(r.pktHooks, h)
	}
	if h, ok := p.(SessionHook); ok {
		r.sessHooks = append(r.sessHooks, h)
	}
}

// Start 依次启动。
func (r *Registry) Start(ctx context.Context) error {
	r.mu.RLock()
	plugins := append([]Plugin(nil), r.plugins...)
	r.mu.RUnlock()
	for _, p := range plugins {
		if err := p.Start(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Stop 反序停止。
func (r *Registry) Stop(ctx context.Context) {
	r.mu.RLock()
	plugins := append([]Plugin(nil), r.plugins...)
	r.mu.RUnlock()
	for i := len(plugins) - 1; i >= 0; i-- {
		_ = plugins[i].Stop(ctx)
	}
}

// FirePacket 按顺序触发包钩子。
func (r *Registry) FirePacket(s *session.Session, dir java.Direction, p *java.Packet) PacketAction {
	r.mu.RLock()
	hooks := r.pktHooks
	r.mu.RUnlock()
	for _, h := range hooks {
		switch h.OnPacket(s, dir, p) {
		case PacketDrop:
			return PacketDrop
		case PacketModified:
			return PacketModified
		}
	}
	return PacketPass
}

// FireSession 触发会话事件。
func (r *Registry) FireSession(s *session.Session, ev SessionEvent) {
	r.mu.RLock()
	hooks := r.sessHooks
	r.mu.RUnlock()
	for _, h := range hooks {
		h.OnSessionEvent(s, ev)
	}
}
