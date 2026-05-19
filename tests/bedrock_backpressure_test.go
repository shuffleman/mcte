package tests

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/shuffleman/mcte/pkg/protocol/bedrock/raknet"
)

// noopPacketConn 一个最小的 net.PacketConn 用于构造 Session。
type noopPacketConn struct{}

func (noopPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	time.Sleep(time.Hour)
	return 0, nil, nil
}
func (noopPacketConn) WriteTo(p []byte, addr net.Addr) (int, error) { return len(p), nil }
func (noopPacketConn) Close() error                                  { return nil }
func (noopPacketConn) LocalAddr() net.Addr                           { return &net.UDPAddr{} }
func (noopPacketConn) SetDeadline(t time.Time) error                 { return nil }
func (noopPacketConn) SetReadDeadline(t time.Time) error             { return nil }
func (noopPacketConn) SetWriteDeadline(t time.Time) error            { return nil }

// 让 session 进入 connected 状态后，构造一个需要 ACK 的应用层 reliable ordered datagram，
// 通过 Feed 注入，并停止消费 recvCh —— 验证不丢应用层包，超时由 idle 关闭。
func TestSessionDeliverBlocksOnSlowConsumer(t *testing.T) {
	addr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345}
	sess := raknet.NewSession(noopPacketConn{}, addr, 1492, 12345)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// idle 1s 触发关闭
	go sess.RunBackground(ctx, 1*time.Second)

	// 注入一个 reliable ordered datagram，含一个 0x80 开头的应用层 packet body
	// datagram 头：FlagValid + 24-bit seq=0
	// encapsulated：flag = (RelReliableOrdered=3)<<5 + 0(no frag) = 0x60
	// bitLen = 8 (1 byte body)
	// MsgIndex 3B + OrderIndex 3B + OrderChan 1B + body 1B
	dg := []byte{
		0x84,             // FlagValid
		0, 0, 0,          // seq 0 LE 24-bit
		0x60,             // reliability=ReliableOrdered, no frag
		0, 8,             // bitLen=8 BE
		0, 0, 0,          // msgIndex
		0, 0, 0, 0,       // orderIndex + orderChan
		0x80,             // body
	}
	if err := sess.Feed(dg); err != nil {
		t.Fatalf("Feed: %v", err)
	}

	// 不消费 recvCh；等 idle 超时关闭 session
	select {
	case <-sess.Closed():
	case <-time.After(3 * time.Second):
		t.Fatalf("session did not close on idle")
	}
}
