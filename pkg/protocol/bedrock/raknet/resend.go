package raknet

import (
	"context"
	"sync"
	"time"
)

// resendEntry 已发送但未收到 ACK 的 datagram。
type resendEntry struct {
	seq      uint32
	payload  []byte // 完整 datagram（含首字节 flag + 3B seq）
	sent     time.Time
	attempts int
}

// ResendQueue 维护在飞数据，并以 cwnd 暴露发送侧背压。
//
// 拥塞控制：标准 slow start + AIMD。
//   - cwnd 初值 minCwnd；每收到一个新 ACK：
//     · inflight < ssthresh 时 cwnd += 1 (slow start)
//     · 否则每 cwnd 个 ACK cwnd += 1     (congestion avoidance)
//   - 收到 NAK 或 RTO 触发重传时 ssthresh = max(cwnd/2, minCwnd)，cwnd = ssthresh
//   - 永远不超过 maxCwnd
type ResendQueue struct {
	mu      sync.Mutex
	entries map[uint32]*resendEntry
	rto     time.Duration
	maxTry  int

	cwnd      int
	ssthresh  int
	caCounter int // congestion avoidance 累计 ACK 计数
	minCwnd   int
	maxCwnd   int

	// 每次空间变化时关闭并重建，唤醒所有 WaitForRoom 等待者
	roomCh chan struct{}

	dropAttempts uint64 // 重试用尽丢弃的条目数
}

func NewResendQueue(rto time.Duration, maxTry int) *ResendQueue {
	if rto == 0 {
		rto = 200 * time.Millisecond
	}
	if maxTry == 0 {
		maxTry = 24
	}
	return &ResendQueue{
		entries:  make(map[uint32]*resendEntry),
		rto:      rto,
		maxTry:   maxTry,
		cwnd:     32,
		ssthresh: 256,
		minCwnd:  4,
		maxCwnd:  2048,
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

// Add 加入新发送的 datagram。
func (q *ResendQueue) Add(seq uint32, payload []byte) {
	cp := make([]byte, len(payload))
	copy(cp, payload)
	q.mu.Lock()
	q.entries[seq] = &resendEntry{seq: seq, payload: cp, sent: time.Now()}
	q.mu.Unlock()
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

// NakRange 收到 NAK，立即返回需要重传的 payloads；触发 cwnd 减半。
func (q *ResendQueue) NakRange(records []AckRecord) [][]byte {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := [][]byte{}
	naked := 0
	for _, r := range records {
		for s := r.Start; s <= r.End; s++ {
			if e, ok := q.entries[s]; ok {
				e.attempts++
				e.sent = time.Now()
				out = append(out, e.payload)
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

// DueForResend 返回 RTO 超时仍未 ACK 的 payloads，并触发 cwnd 减半。
func (q *ResendQueue) DueForResend(now time.Time) [][]byte {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := [][]byte{}
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
			out = append(out, e.payload)
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

// onAck 假设持锁；按 slow start / CA 增长 cwnd。
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

// onLoss 持锁；丢包时减半。
func (q *ResendQueue) onLoss() {
	q.ssthresh = q.cwnd / 2
	if q.ssthresh < q.minCwnd {
		q.ssthresh = q.minCwnd
	}
	q.cwnd = q.ssthresh
	q.caCounter = 0
}

// broadcastRoom 持锁；唤醒所有等 cwnd 的 waiter。
func (q *ResendQueue) broadcastRoom() {
	close(q.roomCh)
	q.roomCh = make(chan struct{})
}
