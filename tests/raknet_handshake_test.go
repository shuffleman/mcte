package tests

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/shuffleman/mcte/pkg/protocol/bedrock/raknet"
)

// TestRakNetDialHandshake 验证 RakNet 客户端能与真实 server 完成完整握手：
//   1. OCR1 → Reply1
//   2. OCR2 → Reply2
//   3. ConnectionRequest (reliable) → CRA (reliable ordered)  ← bug 曾卡死在这一步
//   4. NewIncomingConnection
//   5. 双方都进入 Connected 状态
//
// 用 net.PacketConn 起一对 UDP socket 完成实拨号 + 握手。
func TestRakNetDialHandshake(t *testing.T) {
	// 起一个 UDP server socket
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer pc.Close()
	serverAddr := pc.LocalAddr().(*net.UDPAddr)
	serverGUID := raknet.GenServerGUID()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 模拟一个最简 RakNet server：UDP 分发到对应 Session
	sessions := make(map[string]*raknet.Session)
	sessCh := make(chan *raknet.Session, 1)
	go func() {
		buf := make([]byte, 2048)
		for {
			n, addr, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			payload := make([]byte, n)
			copy(payload, buf[:n])
			key := addr.String()
			sess, ok := sessions[key]
			if !ok {
				if payload[0] != raknet.IDOpenConnectionRequest1 {
					continue
				}
				sess = raknet.NewSession(pc, addr, 1492, serverGUID)
				sessions[key] = sess
				go sess.RunBackground(ctx, 30*time.Second)
				select {
				case sessCh <- sess:
				default:
				}
			}
			_ = sess.Feed(payload)
		}
	}()

	// 客户端拨号
	dialCtx, dialCancel := context.WithTimeout(ctx, 5*time.Second)
	defer dialCancel()
	clientSess, err := raknet.Dial(dialCtx, serverAddr.String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer clientSess.Close()

	if clientSess.State() != raknet.StateConnected {
		t.Fatalf("client not connected: state=%v", clientSess.State())
	}

	// 等 server session 也进入 Connected
	select {
	case sess := <-sessCh:
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if sess.State() == raknet.StateConnected {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("server session never reached Connected: state=%v", sess.State())
	case <-time.After(3 * time.Second):
		t.Fatalf("server never saw session")
	}
}
