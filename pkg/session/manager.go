package session

import (
	"context"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

const shardCount = 16

// Manager 用 16 shard sync.Map 路由会话；按 ID 取模分片。
type Manager struct {
	shards [shardCount]sync.Map
	count  atomic.Int64

	idleTimeout   time.Duration
	maxConcurrent int64
	nextID        atomic.Uint64
}

func NewManager(idleTimeout time.Duration, maxConcurrent int) *Manager {
	if idleTimeout == 0 {
		idleTimeout = 5 * time.Minute
	}
	return &Manager{idleTimeout: idleTimeout, maxConcurrent: int64(maxConcurrent)}
}

// NewID 分配新会话 ID。
func (m *Manager) NewID() uint64 { return m.nextID.Add(1) }

// Add 注册会话。返回 false 表示超过上限。
func (m *Manager) Add(s *Session) bool {
	if m.maxConcurrent > 0 && m.count.Load() >= m.maxConcurrent {
		return false
	}
	m.shardOf(s.ID).Store(s.ID, s)
	m.count.Add(1)
	return true
}

// Remove 注销会话。
func (m *Manager) Remove(id uint64) {
	if _, loaded := m.shardOf(id).LoadAndDelete(id); loaded {
		m.count.Add(-1)
	}
}

// Get 通过 ID 查找。
func (m *Manager) Get(id uint64) (*Session, bool) {
	v, ok := m.shardOf(id).Load(id)
	if !ok {
		return nil, false
	}
	return v.(*Session), true
}

// FindByRemoteIP 扫描所有 shard，慢路径。
func (m *Manager) FindByRemoteIP(ip string) []*Session {
	out := []*Session{}
	for i := range m.shards {
		m.shards[i].Range(func(_, v any) bool {
			s := v.(*Session)
			if s.RemoteAddr != nil {
				h := s.RemoteAddr.String()
				if hostOf(h) == ip {
					out = append(out, s)
				}
			}
			return true
		})
	}
	return out
}

// Count 当前数量。
func (m *Manager) Count() int64 { return m.count.Load() }

// RunSweeper 启动空闲会话回收 goroutine。
func (m *Manager) RunSweeper(ctx context.Context, onSweep func(*Session)) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cutoff := time.Now().Add(-m.idleTimeout)
			for i := range m.shards {
				m.shards[i].Range(func(_, v any) bool {
					s := v.(*Session)
					if s.LastActive().Before(cutoff) {
						if onSweep != nil {
							onSweep(s)
						}
						m.Remove(s.ID)
					}
					return true
				})
			}
		}
	}
}

func (m *Manager) shardOf(id uint64) *sync.Map {
	return &m.shards[id%shardCount]
}

func hostOf(addr string) string {
	for i := len(addr) - 1; i >= 0; i-- {
		if addr[i] == ':' {
			return addr[:i]
		}
	}
	return addr
}

// Hash 工具：供外部按字符串 key 取分片。
func Hash(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}
