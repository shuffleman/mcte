package raknet

import (
	"container/list"
	"errors"
	"sync"
	"time"
)

// FragmentAssembler 按 compoundID 合并分片。
//
// 抗内存放大攻击：
//   - groups 数量硬上限 maxGroups（默认 64）；超出时 LRU 淘汰最旧。
//   - 单个 group 的 total 不预 alloc slice，按 index 写入 map；即使声明 total=65536，
//     未到达的索引也不占内存。
//   - 每个 group 携带创建时间，由 GC 定时器（idleTTL，默认 5s）扫描淘汰未完成的。
//   - 单 group 累计字节超过 maxSize 立即丢弃。
//   - 单 fragment.total 超过 maxTotal 直接拒绝（默认 4096，远小于 65536）。
type FragmentAssembler struct {
	mu        sync.Mutex
	groups    map[uint16]*fragmentGroup
	lru       *list.List
	maxSize   int
	maxGroups int
	maxTotal  uint32
	idleTTL   time.Duration

	dropMaxGroups uint64
	dropTimeout   uint64
	dropMaxTotal  uint64
	dropMaxSize   uint64
}

type fragmentGroup struct {
	id        uint16
	total     uint32
	received  uint32
	bytes     int
	parts     map[uint32][]byte
	createdAt time.Time
	elem      *list.Element // LRU 节点
}

// NewFragmentAssembler 构造分片合并器。
//   maxBytes:  单 compound 最大累计字节（0 表示 16MB 默认）
func NewFragmentAssembler(maxBytes int) *FragmentAssembler {
	if maxBytes <= 0 {
		maxBytes = 16 * 1024 * 1024
	}
	return &FragmentAssembler{
		groups:    make(map[uint16]*fragmentGroup),
		lru:       list.New(),
		maxSize:   maxBytes,
		maxGroups: 64,
		maxTotal:  4096,
		idleTTL:   5 * time.Second,
	}
}

// Add 收入一片；若拼装完成返回完整数据与 true。
func (a *FragmentAssembler) Add(compoundID uint16, index, total uint32, body []byte) ([]byte, bool, error) {
	if total == 0 {
		return nil, false, errors.New("raknet: invalid fragment total")
	}
	if total > a.maxTotal {
		a.mu.Lock()
		a.dropMaxTotal++
		a.mu.Unlock()
		return nil, false, errors.New("raknet: fragment total exceeds limit")
	}
	if index >= total {
		return nil, false, errors.New("raknet: fragment index out of range")
	}

	a.mu.Lock()
	g, ok := a.groups[compoundID]
	if !ok {
		// LRU 淘汰
		for len(a.groups) >= a.maxGroups {
			oldest := a.lru.Back()
			if oldest == nil {
				break
			}
			og := oldest.Value.(*fragmentGroup)
			a.lru.Remove(oldest)
			delete(a.groups, og.id)
			a.dropMaxGroups++
		}
		g = &fragmentGroup{
			id:        compoundID,
			total:     total,
			parts:     make(map[uint32][]byte, 8),
			createdAt: time.Now(),
		}
		g.elem = a.lru.PushFront(g)
		a.groups[compoundID] = g
	} else {
		// 已存在时更新 LRU 位置
		a.lru.MoveToFront(g.elem)
		// 若客户端在同一 compoundID 内修改 total，直接丢弃整个 group 防止混乱
		if g.total != total {
			a.lru.Remove(g.elem)
			delete(a.groups, compoundID)
			a.mu.Unlock()
			return nil, false, errors.New("raknet: fragment total mismatch")
		}
	}
	if _, dup := g.parts[index]; dup {
		a.mu.Unlock()
		return nil, false, nil
	}
	g.parts[index] = body
	g.received++
	g.bytes += len(body)
	if g.bytes > a.maxSize {
		a.lru.Remove(g.elem)
		delete(a.groups, compoundID)
		a.dropMaxSize++
		a.mu.Unlock()
		return nil, false, errors.New("raknet: fragment compound too large")
	}
	if g.received < g.total {
		a.mu.Unlock()
		return nil, false, nil
	}
	// 拼装完成
	a.lru.Remove(g.elem)
	delete(a.groups, compoundID)
	a.mu.Unlock()

	out := make([]byte, 0, g.bytes)
	for i := uint32(0); i < g.total; i++ {
		p, ok := g.parts[i]
		if !ok {
			return nil, false, errors.New("raknet: fragment hole")
		}
		out = append(out, p...)
	}
	return out, true, nil
}

// SweepExpired 清理超过 idleTTL 仍未完成的 group。建议每秒调用一次。
func (a *FragmentAssembler) SweepExpired(now time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	cutoff := now.Add(-a.idleTTL)
	for e := a.lru.Back(); e != nil; {
		g := e.Value.(*fragmentGroup)
		if !g.createdAt.Before(cutoff) {
			break
		}
		prev := e.Prev()
		a.lru.Remove(e)
		delete(a.groups, g.id)
		a.dropTimeout++
		e = prev
	}
}

// Stats 监控指标。
func (a *FragmentAssembler) Stats() (groups int, dropMaxGroups, dropTimeout, dropMaxTotal, dropMaxSize uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.groups), a.dropMaxGroups, a.dropTimeout, a.dropMaxTotal, a.dropMaxSize
}
