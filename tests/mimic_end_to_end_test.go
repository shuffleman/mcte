package tests

import (
	"context"
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

// 启动开启 mimic 的 inbound + 开启 mimic 的客户端 SDK，验证多 size 数据端到端可达。
func TestMimicEndToEnd(t *testing.T) {
	// 1. echo TCP server
	echoLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("echo listen: %v", err)
	}
	defer echoLn.Close()
	var echoWg sync.WaitGroup
	echoWg.Add(1)
	go func() {
		defer echoWg.Done()
		c, err := echoLn.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		_, _ = io.Copy(c, c)
	}()
	echoAddr := echoLn.Addr().(*net.TCPAddr)

	// 2. MCTE inbound（开启 mimic）
	userID := uuid.New()
	cfg := config.Defaults()
	cfg.Listen.TCP = "127.0.0.1:46566"
	cfg.Listen.UDP = ""
	cfg.Users = []config.UserConfig{{Name: "alice", UUID: userID.String()}}
	cfg.Fallback.Targets = []string{"127.0.0.1:1"}
	cfg.Tunnel.Mimic.Enabled = true
	eng, err := engine.New(&cfg, zap.NewNop(), engine.DefaultUpstreamDialer{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("engine new: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = eng.Run(ctx) }()
	time.Sleep(200 * time.Millisecond)

	// 3. client SDK 开启 mimic
	sdk, err := client.New(client.Config{
		Server:  "127.0.0.1",
		Port:    46566,
		UUID:    userID.String(),
		Network: "tcp",
		Mimic:   client.DefaultProfile(),
	})
	if err != nil {
		t.Fatalf("client new: %v", err)
	}
	conn, err := sdk.Dial(ctx, echoAddr.IP.String(), uint16(echoAddr.Port))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// 不同 size 各跑一遍：模拟 move / fly / chunk 行为
	for _, size := range []int{8, 100, 4000} {
		msg := make([]byte, size)
		for i := range msg {
			msg[i] = byte(i*7 + size)
		}
		if _, err := conn.Write(msg); err != nil {
			t.Fatalf("write size=%d: %v", size, err)
		}
		buf := make([]byte, size)
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		if _, err := io.ReadFull(conn, buf); err != nil {
			t.Fatalf("read size=%d: %v", size, err)
		}
		for i := range msg {
			if buf[i] != msg[i] {
				t.Fatalf("byte mismatch size=%d at %d", size, i)
			}
		}
	}
}
