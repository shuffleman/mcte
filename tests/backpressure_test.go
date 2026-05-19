package tests

import (
	"context"
	"testing"
	"time"

	"github.com/shuffleman/mcte/pkg/protocol/bedrock/raknet"
)

// ResendQueue cwnd 初值与限流。
func TestResendQueueWaitForRoom(t *testing.T) {
	q := raknet.NewResendQueue(200*time.Millisecond, 8)
	// 初始 cwnd 是 32，应当能立即拿到空间
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := q.WaitForRoom(ctx); err != nil {
		t.Fatalf("initial WaitForRoom should not block: %v", err)
	}
}

// 填满 cwnd 后 WaitForRoom 必须阻塞直到 AckRange 释放。
func TestResendQueueBlockUntilAck(t *testing.T) {
	q := raknet.NewResendQueue(200*time.Millisecond, 8)
	// 填满
	for i := 0; i < 32; i++ {
		q.Add(uint32(i), []byte{0x84, 0, 0, 0})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := q.WaitForRoom(ctx)
	if err == nil {
		t.Fatalf("expected ctx timeout, got nil")
	}

	// 释放一个，等待应当成功
	go func() {
		time.Sleep(20 * time.Millisecond)
		q.AckRange([]raknet.AckRecord{{Start: 0, End: 0}})
	}()
	ctx2, cancel2 := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel2()
	if err := q.WaitForRoom(ctx2); err != nil {
		t.Fatalf("expected room after ack, got %v", err)
	}
}

// NAK 与 RTO 触发 cwnd 减半。
func TestResendQueueCongestionControl(t *testing.T) {
	q := raknet.NewResendQueue(50*time.Millisecond, 8)
	for i := 0; i < 32; i++ {
		q.Add(uint32(i), []byte{0x84, 0, 0, 0})
	}
	before := q.Cwnd()
	q.NakRange([]raknet.AckRecord{{Start: 0, End: 0}})
	after := q.Cwnd()
	if after >= before {
		t.Fatalf("expected cwnd to shrink: before=%d after=%d", before, after)
	}
}

// AckCollector 满返回 false。
func TestAckCollectorCapacity(t *testing.T) {
	c := raknet.NewAckCollector()
	// 4096 阈值
	var fullSeen bool
	for i := 0; i < 5000; i++ {
		if !c.Add(uint32(i)) {
			fullSeen = true
			break
		}
	}
	if !fullSeen {
		t.Fatalf("expected AckCollector to signal full")
	}
}

// OrderingBuffer 超容量强制跳过 expected。
func TestOrderingBufferOverflow(t *testing.T) {
	b := raknet.NewOrderingBuffer()
	// 制造长缺洞：order index 跳过 0，从 1 起塞 1500 条
	for i := uint32(1); i < 1500; i++ {
		b.Push(raknet.EncapsulatedPacket{
			Reliability: raknet.RelReliableOrdered,
			OrderIndex:  i,
			Body:        []byte{0x80},
		})
	}
	if b.Skipped() == 0 {
		t.Fatalf("expected skipped > 0")
	}
}
