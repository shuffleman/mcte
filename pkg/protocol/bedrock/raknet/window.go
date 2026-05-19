package raknet

import (
	"sort"
	"sync"
)

// SeqWindow 维护一个 24-bit 序号窗口，用于去重接收。
type SeqWindow struct {
	mu      sync.Mutex
	seen    map[uint32]struct{}
	highest uint32
	size    uint32
}

func NewSeqWindow(size uint32) *SeqWindow {
	if size == 0 {
		size = 2048
	}
	return &SeqWindow{seen: make(map[uint32]struct{}, size), size: size}
}

// Receive 返回 true 表示首次见到该序号。
func (w *SeqWindow) Receive(seq uint32) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.seen[seq]; ok {
		return false
	}
	w.seen[seq] = struct{}{}
	if seq > w.highest {
		w.highest = seq
	}
	if uint32(len(w.seen)) > w.size {
		var threshold uint32
		if w.highest > w.size {
			threshold = w.highest - w.size
		}
		for k := range w.seen {
			if k < threshold {
				delete(w.seen, k)
			}
		}
	}
	return true
}

// MissingBefore 返回 [0, highest] 内未收到的序号（用于发 NAK）。
func (w *SeqWindow) MissingBefore(maxN int) []uint32 {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := []uint32{}
	if w.highest == 0 {
		return out
	}
	for i := uint32(0); i <= w.highest && len(out) < maxN; i++ {
		if _, ok := w.seen[i]; !ok {
			out = append(out, i)
		}
	}
	return out
}

// AckCollector 累积已收到的序号，定时编码为 ACK 发回。
// 容量上限：超过 maxPending 时 Add 会返回 false 通知调用方应立即触发 flush。
type AckCollector struct {
	mu         sync.Mutex
	set        map[uint32]struct{}
	maxPending int
}

func NewAckCollector() *AckCollector {
	return &AckCollector{set: make(map[uint32]struct{}), maxPending: 4096}
}

// Add 返回 true 表示加入成功；false 表示已满，调用方应立即 Flush 发包。
func (a *AckCollector) Add(seq uint32) bool {
	a.mu.Lock()
	a.set[seq] = struct{}{}
	full := len(a.set) >= a.maxPending
	a.mu.Unlock()
	return !full
}

// Flush 取出当前累积的序号并清空。
func (a *AckCollector) Flush() []uint32 {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.set) == 0 {
		return nil
	}
	out := make([]uint32, 0, len(a.set))
	for k := range a.set {
		out = append(out, k)
	}
	a.set = make(map[uint32]struct{})
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// OrderingBuffer 单 ordering channel 内的乱序重排序缓冲。
// pending 容量上限 maxPending；超出时强制跳过 expected 释放最旧条目，
// 防止恶意客户端构造长缺洞使内存耗尽。
type OrderingBuffer struct {
	expected   uint32
	pending    map[uint32][]EncapsulatedPacket
	maxPending int
	skipped    uint64
}

func NewOrderingBuffer() *OrderingBuffer {
	return &OrderingBuffer{
		pending:    make(map[uint32][]EncapsulatedPacket),
		maxPending: 1024,
	}
}

// Push 收一个 ordered 包，返回当前可顺序释放的包数组。
func (o *OrderingBuffer) Push(ep EncapsulatedPacket) []EncapsulatedPacket {
	if ep.OrderIndex < o.expected {
		return nil
	}
	o.pending[ep.OrderIndex] = append(o.pending[ep.OrderIndex], ep)

	// 超容量：强制推进 expected 到 pending 中最小的 OrderIndex，并释放可释放的连续块
	if len(o.pending) > o.maxPending {
		var minIdx uint32
		first := true
		for k := range o.pending {
			if first || k < minIdx {
				minIdx = k
				first = false
			}
		}
		if minIdx > o.expected {
			o.skipped += uint64(minIdx - o.expected)
			o.expected = minIdx
		}
	}

	var out []EncapsulatedPacket
	for {
		eps, ok := o.pending[o.expected]
		if !ok {
			break
		}
		delete(o.pending, o.expected)
		out = append(out, eps...)
		o.expected++
	}
	return out
}

// Skipped 因超容量被跳过的 order index 数（监控用）。
func (o *OrderingBuffer) Skipped() uint64 { return o.skipped }
