package tests

import (
	"context"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/shuffleman/mcte/pkg/client"
	"github.com/shuffleman/mcte/pkg/config"
	"github.com/shuffleman/mcte/pkg/engine"
)

// TestJavaEndToEnd:
//   1. 启动一个 echo TCP server 作为"上游"目标
//   2. 启动 MCTE engine 监听本地 TCP 端口（带一个 user UUID）
//   3. 用 client SDK 拨号 → 发数据 → 期望 echo 返回
//
// 这是 inbound + outbound 端到端的最小验证。
func TestJavaEndToEnd(t *testing.T) {
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

	// 2. MCTE inbound
	user := uuid.New()
	cfg := config.Defaults()
	cfg.Listen.TCP = "127.0.0.1:0" // 随机端口
	cfg.Listen.UDP = ""             // 仅 Java
	cfg.Users = []config.UserConfig{{Name: "alice", UUID: user.String()}}
	cfg.Fallback.Targets = []string{"127.0.0.1:1"} // 没用到，但要通过 validate
	// MCTE engine 内部需要监听后才能获得实际端口；用一个固定端口
	// 这里改用先 listen 拿端口，再传给 engine 不方便；直接配一个高位端口
	cfg.Listen.TCP = "127.0.0.1:46565"
	eng, err := engine.New(&cfg, zap.NewNop(), engine.DefaultUpstreamDialer{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("engine new: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = eng.Run(ctx) }()

	// 等 engine 起来
	time.Sleep(200 * time.Millisecond)

	// 3. client SDK 拨号
	sdk, err := client.New(client.Config{
		Server:  "127.0.0.1",
		Port:    46565,
		UUID:    user.String(),
		Network: "tcp",
	})
	if err != nil {
		t.Fatalf("client new: %v", err)
	}
	conn, err := sdk.Dial(ctx, echoAddr.IP.String(), uint16(echoAddr.Port))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	msg := []byte("hello-mcte-end-to-end")
	if _, err := conn.Write(msg); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, len(msg))
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf) != string(msg) {
		t.Fatalf("echo mismatch: got %q want %q", buf, msg)
	}
}

// 解析 listenAddress（保留备用）。
func _addrToPort(addr string) uint16 {
	_, port, _ := net.SplitHostPort(addr)
	p, _ := strconv.Atoi(port)
	return uint16(p)
}
