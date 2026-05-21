package raknet

import (
	"context"
	"sync"
	"time"
)

// resendEntry 已发送但未收到 ACK 的 datagram。
//
// 关键设计：存的是 EncapsulatedPacket 列表（不是原 datagram 字节）。
// 重传时调用方应当**用新的 datagram seq**重新封包发出 — 因为某些路径性丢包
// （运营商/路由器按 5-tuple+payload hash 持续丢同一字节序列）会导致原 seq 的
// 重传永远到不了；新 seq 通常能避开。
type resendEntry struct {
	originalSeq uint32                // 原始 datagram seq（用于查找/移除）
	eps         []EncapsulatedPacket // 该 datagram 内的 EP（重传时复用 MsgIndex/OrderIndex 等）
	sent        time.Time
	attempts    int
}

// ResendQueue 维护在飞数据，并以 cwnd 暴露发送侧背压。
type ResendQueue struct {
	mu      sync.Mutex
	entries map[uint32]*resendEntry
	rto     time.Duration
	maxTry  int

	cwnd      int
	ssthresh  int
	caCounter int
	minCwnd   int
	maxCwnd   int

	roomCh chan struct{}

	dropAttempts uint64
}

func NewResendQueue(rto time.Duration, maxTry int) *ResendQueue {
	if rto == 0 {
		rto = 200 * time.Millisecond
	}
	if maxTry == 0 {
		// 跨境/无线链路 UDP 丢包率常达 20%，maxTry=24 (4.8s) 不够 — 实测跨网络 100 次
		// 中 33 次因单 EP 重传用尽 close session。提高到 60 (12s) 给单 EP 99.99% 成功率。
		maxTry = 60
	}
	// 拥塞窗口：maxCwnd=128 是实测 sweet spot。
	// 跨境 RTT~90ms 链路实测：maxCwnd=64 -> 3.2s/req, =128 -> 2.0s/req, =256 -> 3.1s/req。
	// 256 反而慢说明大 burst 触发上游延迟（非丢包）。128 是兼顾速率与稳定的最优值。
	return &ResendQueue{
		entries:  make(map[uint32]*resendEntry),
		rto:      rto,
		maxTry:   maxTry,
		cwnd:     16,
		ssthresh: 64,
		minCwnd:  4,
		maxCwnd:  128,
		roomCh:   make(chan struct{}),
	}
}

// WaitForRoom 阻塞直到 in-flight 数量 < cwnd，或 ctx 取消。
func (q *ResendQueue) WaitForRoom(ctx context.Context) error {
	for {
		q.mu.Lock()
		if len(q.entries) < q.cwnd {
			q.mu.Unlock()
			return nil
		}
		ch := q.roomCh
		q.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Add 加入新发送的 datagram。eps 必须不再被外部修改（调用方传所有权）。
func (q *ResendQueue) Add(seq uint32, eps []EncapsulatedPacket) {
	q.mu.Lock()
	q.entries[seq] = &resendEntry{
		originalSeq: seq,
		eps:         eps,
		sent:        time.Now(),
	}
	q.mu.Unlock()
}

// Rekey 把 entry 从 oldSeq 移到 newSeq。用于重传时 datagram seq 变更后维持跟踪。
func (q *ResendQueue) Rekey(oldSeq, newSeq uint32) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if e, ok := q.entries[oldSeq]; ok {
		delete(q.entries, oldSeq)
		e.originalSeq = newSeq
		q.entries[newSeq] = e
	}
}

// AckRange 收到 ACK 范围，移除对应条目并触发 cwnd 增长。
func (q *ResendQueue) AckRange(records []AckRecord) {
	q.mu.Lock()
	removed := 0
	for _, r := range records {
		for s := r.Start; s <= r.End; s++ {
			if _, ok := q.entries[s]; ok {
				delete(q.entries, s)
				removed++
			}
			if s == r.End {
				break
			}
		}
	}
	if removed > 0 {
		q.onAck(removed)
		q.broadcastRoom()
	}
	q.mu.Unlock()
}

// RetransmitItem 表示一项需要重传的数据。
type RetransmitItem struct {
	OriginalSeq uint32
	EPs         []EncapsulatedPacket
}

// NakRange 收到 NAK，立即返回需要重传的 items；触发 cwnd 减半。
// 调用方应当用新 seq 重新发出 EPs，并调 Rekey 更新跟踪。
func (q *ResendQueue) NakRange(records []AckRecord) []RetransmitItem {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := []RetransmitItem{}
	naked := 0
	for _, r := range records {
		for s := r.Start; s <= r.End; s++ {
			if e, ok := q.entries[s]; ok {
				e.attempts++
				e.sent = time.Now()
				out = append(out, RetransmitItem{OriginalSeq: s, EPs: e.eps})
				naked++
			}
			if s == r.End {
				break
			}
		}
	}
	if naked > 0 {
		q.onLoss()
	}
	return out
}

// DueForResend 返回 RTO 超时仍未 ACK 的 items；触发 cwnd 减半。
// 调用方用新 seq 重新发出 EPs 并 Rekey。
func (q *ResendQueue) DueForResend(now time.Time) []RetransmitItem {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := []RetransmitItem{}
	timedOut := 0
	for seq, e := range q.entries {
		if now.Sub(e.sent) >= q.rto {
			e.attempts++
			e.sent = now
			if e.attempts > q.maxTry {
				delete(q.entries, seq)
				q.dropAttempts++
				continue
			}
			out = append(out, RetransmitItem{OriginalSeq: seq, EPs: e.eps})
			timedOut++
		}
	}
	if timedOut > 0 {
		q.onLoss()
		q.broadcastRoom()
	}
	return out
}

// Len 当前 in-flight 数量。
func (q *ResendQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.entries)
}

// Cwnd 当前拥塞窗口大小（监控用）。
func (q *ResendQueue) Cwnd() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.cwnd
}

// DropAttempts 重试用尽丢弃的条目数。
func (q *ResendQueue) DropAttempts() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.dropAttempts
}

func (q *ResendQueue) onAck(n int) {
	for i := 0; i < n; i++ {
		if q.cwnd < q.ssthresh {
			q.cwnd++
		} else {
			q.caCounter++
			if q.caCounter >= q.cwnd {
				q.caCounter = 0
				q.cwnd++
			}
		}
		if q.cwnd > q.maxCwnd {
			q.cwnd = q.maxCwnd
		}
	}
}

func (q *ResendQueue) onLoss() {
	q.ssthresh = q.cwnd / 2
	if q.ssthresh < q.minCwnd {
		q.ssthresh = q.minCwnd
	}
	q.cwnd = q.ssthresh
	q.caCounter = 0
}

func (q *ResendQueue) broadcastRoom() {
	close(q.roomCh)
	q.roomCh = make(chan struct{})
}
