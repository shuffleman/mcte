package raknet

import (
	"sort"
	"sync"
)

// SeqWindow 用于 reliable datagram 去重，保留近 N 个 seq 已收记录。
type SeqWindow struct {
	size int
	high uint32
	set  map[uint32]struct{}
}

func NewSeqWindow(size int) *SeqWindow {
	return &SeqWindow{
		size: size,
		set:  make(map[uint32]struct{}),
	}
}

// Receive 标记一个 seq 已收到，返回 true 表示首次收到。
func (w *SeqWindow) Receive(seq uint32) bool {
	if _, ok := w.set[seq]; ok {
		return false
	}
	w.set[seq] = struct{}{}
	if seq > w.high {
		w.high = seq
	}
	// 简易过期：超过 size 的旧 seq 直接清掉
	if len(w.set) > w.size {
		for k := range w.set {
			if k+uint32(w.size) < w.high {
				delete(w.set, k)
			}
		}
	}
	return true
}

// AckCollector 待发 ACK 序号收集器，flush 时合并为 records。
// Add 由 dispatch / inbox goroutine 调用，Flush 由 RunBackground 的 ackTick
// goroutine 调用，必须 mutex 保护 — 否则 fatal: concurrent map iteration and map write。
type AckCollector struct {
	mu  sync.Mutex
	set map[uint32]struct{}
}

func NewAckCollector() *AckCollector {
	return &AckCollector{set: make(map[uint32]struct{})}
}

// Add 加入一个 seq；超过阈值返回 false。
func (a *AckCollector) Add(seq uint32) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.set) >= 4096 {
		return false
	}
	a.set[seq] = struct{}{}
	return true
}

// MissingBefore 返回 [0, highest] 内未收到的序号（用于发 NAK）。
func (a *AckCollector) MissingBefore(highest uint32) []uint32 {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.set) == 0 {
		return nil
	}
	var out []uint32
	for i := uint32(0); i <= highest; i++ {
		if _, ok := a.set[i]; !ok {
			out = append(out, i)
		}
	}
	return out
}

// Flush 取出当前收集的 seq 并清空。
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
//
// pending 容量上限 maxPending；超出时返回 overflow=true 让上层主动断开 session。
// RakNet ReliableOrdered 语义不允许 silent skip — 任何缺失都会让 HTTP 等上层协议
// 拿到不完整数据（curl 看似成功但响应被切一段）。当 pending 长时间积压（意味着
// 某个 OrderIndex 已被 ResendQueue 放弃重传），主动断开让上层协议感知失败并重连。
type OrderingBuffer struct {
	expected   uint32
	pending    map[uint32][]EncapsulatedPacket
	maxPending int
	overflowed bool
}

func NewOrderingBuffer() *OrderingBuffer {
	return &OrderingBuffer{
		pending:    make(map[uint32][]EncapsulatedPacket),
		maxPending: 4096,
	}
}

// Push 收一个 ordered 包，返回当前可顺序释放的包数组及 overflow 标志。
// overflow=true 表示 pending 已超过容量上限，调用方应当立即断开 session。
func (o *OrderingBuffer) Push(ep EncapsulatedPacket) (out []EncapsulatedPacket, overflow bool) {
	if ep.OrderIndex < o.expected {
		return nil, false
	}
	o.pending[ep.OrderIndex] = append(o.pending[ep.OrderIndex], ep)

	for {
		eps, ok := o.pending[o.expected]
		if !ok {
			break
		}
		delete(o.pending, o.expected)
		out = append(out, eps...)
		o.expected++
	}

	if len(o.pending) > o.maxPending {
		o.overflowed = true
		return out, true
	}
	return out, false
}

// Skipped 当 buffer 曾经溢出返回 1，否则 0（测试 / 监控兼容用）。
// 新实现不再 silent skip ordered index：溢出由 Push 的 overflow 返回值通知调用方。
func (o *OrderingBuffer) Skipped() uint64 {
	if o.overflowed {
		return 1
	}
	return 0
}
