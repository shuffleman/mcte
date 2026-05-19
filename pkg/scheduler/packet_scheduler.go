package scheduler

import (
	"container/heap"
	"sync"
)

// Priority 包优先级，数值越小越高。
type Priority int

const (
	PrioKeepAlive Priority = iota
	PrioGameState
	PrioChunk
	PrioEntity
)

// Job 调度单元。
type Job struct {
	Prio  Priority
	Bytes int
	Run   func() error
}

// PacketScheduler 简单 per-session 优先级队列。
type PacketScheduler struct {
	mu sync.Mutex
	q  jobHeap
}

func NewPacketScheduler() *PacketScheduler { return &PacketScheduler{} }

// Enqueue 入队。
func (p *PacketScheduler) Enqueue(j Job) {
	p.mu.Lock()
	defer p.mu.Unlock()
	heap.Push(&p.q, j)
}

// Drain 按 budget 字节预算从队列出队执行。
func (p *PacketScheduler) Drain(budget int) error {
	for budget > 0 {
		p.mu.Lock()
		if p.q.Len() == 0 {
			p.mu.Unlock()
			return nil
		}
		j := heap.Pop(&p.q).(Job)
		p.mu.Unlock()
		if err := j.Run(); err != nil {
			return err
		}
		budget -= j.Bytes
	}
	return nil
}

type jobHeap []Job

func (h jobHeap) Len() int            { return len(h) }
func (h jobHeap) Less(i, j int) bool  { return h[i].Prio < h[j].Prio }
func (h jobHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *jobHeap) Push(x any)         { *h = append(*h, x.(Job)) }
func (h *jobHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
