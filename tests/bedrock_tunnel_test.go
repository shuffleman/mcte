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

// TestBedrockEndToEnd 验证 Bedrock 隧道的字节序保持：
//   1. 起 echo TCP server
//   2. 起 MCTE engine 监听 UDP 19132+TCP（同进程）
//   3. 客户端 SDK 用 network=udp 拨号
//   4. 发送 16KB 随机字节（触发 RakNet 分片）
//   5. 验证 echo 回的字节与发送的完全一致（字节序保持 + 完整性）
//
// 这是之前 TLS 解密失败的复现场景：当大数据被 RakNet 分片 + ordering buffer
// 处理时，bug 会导致字节乱序。
func TestBedrockEndToEnd(t *testing.T) {
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

	// 2. MCTE engine
	userID := uuid.New()
	cfg := config.Defaults()
	cfg.Listen.TCP = ""
	cfg.Listen.UDP = "127.0.0.1:46567"
	cfg.Users = []config.UserConfig{{Name: "alice", UUID: userID.String()}}
	cfg.Fallback.UDP = []string{"127.0.0.1:1"} // 不会用到，但要过校验
	eng, err := engine.New(&cfg, zap.NewNop(), engine.DefaultUpstreamDialer{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("engine new: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = eng.Run(ctx) }()
	time.Sleep(300 * time.Millisecond)

	// 3. client SDK Bedrock
	sdk, err := client.New(client.Config{
		Server:  "127.0.0.1",
		Port:    46567,
		UUID:    userID.String(),
		Network: "udp",
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

	// 4. 不同 size：触发不分片、刚好分片边界、必分片
	for _, size := range []int{128, 1500, 16 * 1024} {
		msg := make([]byte, size)
		if _, err := rand.Read(msg); err != nil {
			t.Fatalf("rand: %v", err)
		}
		// 同 goroutine 写读：先写完再读，避免并发干扰判断
		go func() {
			_, _ = conn.Write(msg)
		}()
		buf := make([]byte, size)
		readDeadline := time.Now().Add(10 * time.Second)
		got := 0
		for got < size {
			if time.Now().After(readDeadline) {
				t.Fatalf("size=%d read timeout, got %d/%d", size, got, size)
			}
			n, rerr := conn.Read(buf[got:])
			if rerr != nil {
				t.Fatalf("size=%d read err at %d/%d: %v", size, got, size, rerr)
			}
			got += n
		}
		// 5. 严格字节序校验
		for i := range msg {
			if buf[i] != msg[i] {
				t.Fatalf("size=%d byte mismatch at %d: got %02x want %02x", size, i, buf[i], msg[i])
			}
		}
	}
}
