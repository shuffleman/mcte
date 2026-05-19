package tests

import (
	"context"
	"crypto/rand"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/shuffleman/mcte/pkg/client"
	"github.com/shuffleman/mcte/pkg/config"
	"github.com/shuffleman/mcte/pkg/engine"
)

// TestBedrockTunnelBidirHeavy 验证 Bedrock 隧道在双向高吞吐下不死锁。
//
// 触发条件：之前版本下"应用层 deliver 阻塞 → dispatchLoop 阻塞 → ACK 也排队等
// → 对端 cwnd 满 → 双向 write 永久阻塞"。修复后 ACK 走 Feed 快速通道，
// 即使 deliver 暂时阻塞也能正常 ACK，无死锁。
func TestBedrockTunnelBidirHeavy(t *testing.T) {
	// echo server
	echoLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("echo listen: %v", err)
	}
	defer echoLn.Close()
	go func() {
		c, err := echoLn.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		_, _ = io.Copy(c, c)
	}()
	echoAddr := echoLn.Addr().(*net.TCPAddr)

	// MCTE engine
	userID := uuid.New()
	cfg := config.Defaults()
	cfg.Listen.TCP = ""
	cfg.Listen.UDP = "127.0.0.1:46568"
	cfg.Users = []config.UserConfig{{Name: "alice", UUID: userID.String()}}
	cfg.Fallback.UDP = []string{"127.0.0.1:1"}
	eng, err := engine.New(&cfg, zap.NewNop(), engine.DefaultUpstreamDialer{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = eng.Run(ctx) }()
	time.Sleep(300 * time.Millisecond)

	sdk, err := client.New(client.Config{
		Server: "127.0.0.1", Port: 46568,
		UUID: userID.String(), Network: "udp",
	})
	if err != nil {
		t.Fatalf("client new: %v", err)
	}
	dialCtx, dialCancel := context.WithTimeout(ctx, 10*time.Second)
	defer dialCancel()
	conn, err := sdk.Dial(dialCtx, echoAddr.IP.String(), uint16(echoAddr.Port))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// 100 个 4KB 消息，双向同时进行。
	const N = 100
	const Size = 4096
	msgs := make([][]byte, N)
	for i := range msgs {
		msgs[i] = make([]byte, Size)
		_, _ = rand.Read(msgs[i])
	}

	var wg sync.WaitGroup
	wg.Add(2)
	errCh := make(chan error, 2)

	// writer
	go func() {
		defer wg.Done()
		for _, m := range msgs {
			if _, err := conn.Write(m); err != nil {
				errCh <- err
				return
			}
		}
	}()

	// reader
	go func() {
		defer wg.Done()
		buf := make([]byte, N*Size)
		got := 0
		_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		for got < N*Size {
			n, rerr := conn.Read(buf[got:])
			if rerr != nil {
				errCh <- rerr
				return
			}
			got += n
		}
		// 严格验证
		for i := 0; i < N; i++ {
			for j := 0; j < Size; j++ {
				if buf[i*Size+j] != msgs[i][j] {
					errCh <- &corruptErr{i: i, j: j}
					return
				}
			}
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case err := <-errCh:
		t.Fatalf("error: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatalf("deadlock or stuck: 60s without completion")
	}
}

type corruptErr struct{ i, j int }

func (e *corruptErr) Error() string { return "byte corruption" }
