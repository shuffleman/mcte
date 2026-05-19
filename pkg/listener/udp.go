package listener

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shuffleman/mcte/pkg/protocol/bedrock/raknet"
)

// UDPListener 维护 net.PacketConn，按 remote addr 多路复用 RakNet 会话。
//
// 抗 session 爆炸：
//   - sessions 数量硬上限 maxSessions（默认 10000）；超出直接丢弃新 OCR1
//   - 半开 session（尚未 Connected）使用更短 idle 超时（10s），降低伪造 OCR1 的内存占用
//   - 完成握手的 session 使用 idleTimeout
type UDPListener struct {
	pc         net.PacketConn
	serverGUID int64
	motd       string

	mu       sync.Mutex
	sessions map[string]*raknet.Session
	ch       chan *raknet.Session

	maxSessions      int
	halfOpenTimeout  time.Duration
	idleTimeout      time.Duration

	dropMaxSessions atomic.Uint64
	dropNotOCR1     atomic.Uint64
}

// Options UDP listener 配置。
type Options struct {
	MOTD            string
	IdleTimeout     time.Duration
	HalfOpenTimeout time.Duration
	MaxSessions     int
}

func NewUDPListener(addr string, queue int, opt Options) (*UDPListener, error) {
	pc, err := net.ListenPacket("udp", addr)
	if err != nil {
		return nil, err
	}
	if queue <= 0 {
		queue = 256
	}
	motd := opt.MOTD
	if motd == "" {
		// 默认 MOTD 必须无品牌特征，避免离线 ping 直接暴露代理身份。
		// Bedrock 服务端常见默认是 "Dedicated Server" / "A Minecraft Server" 等。
		// 字段：MCPE;<motd>;<protocol>;<gameVersion>;<online>;<max>;<serverGUID>;<sub-motd>;<gamemode>;<gamemodeNum>;<portV4>;<portV6>;
		motd = "MCPE;Dedicated Server;800;1.21.4;0;100;0;Bedrock level;Survival;1;19132;19133;"
	}
	maxSessions := opt.MaxSessions
	if maxSessions <= 0 {
		maxSessions = 10000
	}
	halfOpenTimeout := opt.HalfOpenTimeout
	if halfOpenTimeout <= 0 {
		halfOpenTimeout = 10 * time.Second
	}
	idleTimeout := opt.IdleTimeout
	if idleTimeout <= 0 {
		idleTimeout = 30 * time.Second
	}
	return &UDPListener{
		pc:              pc,
		serverGUID:      raknet.GenServerGUID(),
		motd:            motd,
		sessions:        make(map[string]*raknet.Session),
		ch:              make(chan *raknet.Session, queue),
		maxSessions:     maxSessions,
		halfOpenTimeout: halfOpenTimeout,
		idleTimeout:     idleTimeout,
	}, nil
}

func (u *UDPListener) Accept() <-chan *raknet.Session { return u.ch }
func (u *UDPListener) Addr() net.Addr                  { return u.pc.LocalAddr() }

// DropStats 监控指标。
func (u *UDPListener) DropStats() (maxSessions, notOCR1 uint64) {
	return u.dropMaxSessions.Load(), u.dropNotOCR1.Load()
}

// Run 读 UDP 包，按 remote 分发到 Session。
func (u *UDPListener) Run(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		_ = u.pc.Close()
	}()
	buf := make([]byte, 2048)
	for {
		n, addr, err := u.pc.ReadFrom(buf)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if n == 0 {
			continue
		}
		payload := make([]byte, n)
		copy(payload, buf[:n])

		// 离线 ping 直接回 pong，无需建 session
		if payload[0] == raknet.IDUnconnectedPing || payload[0] == raknet.IDUnconnectedPingOpen {
			if p, perr := raknet.ParseUnconnectedPing(payload); perr == nil {
				_, _ = u.pc.WriteTo(raknet.BuildUnconnectedPong(p.PingTime, u.serverGUID, u.motd), addr)
			}
			continue
		}

		key := addr.String()
		u.mu.Lock()
		sess, ok := u.sessions[key]
		if !ok {
			// 只有 OCR1 才允许建立新 Session
			if payload[0] != raknet.IDOpenConnectionRequest1 {
				u.mu.Unlock()
				u.dropNotOCR1.Add(1)
				continue
			}
			// session 数量上限
			if len(u.sessions) >= u.maxSessions {
				u.mu.Unlock()
				u.dropMaxSessions.Add(1)
				continue
			}
			sess = raknet.NewSession(u.pc, addr, 1492, u.serverGUID)
			u.sessions[key] = sess
			halfOpen := u.halfOpenTimeout
			full := u.idleTimeout
			go func(s *raknet.Session) {
				// 半开阶段使用短超时；handshake 完成后切换到正常 idle
				runHalfOpen := time.AfterFunc(halfOpen, func() {
					if s.State() != raknet.StateConnected {
						_ = s.Close()
					}
				})
				defer runHalfOpen.Stop()
				go func() {
					select {
					case <-s.HandshakeDone():
						runHalfOpen.Stop()
					case <-s.Closed():
					}
				}()
				s.RunBackground(ctx, full)
				u.removeSession(s.Remote().String())
			}(sess)
			u.mu.Unlock()
			_ = sess.Feed(payload)
			// 推送给上层做 detector / handler
			select {
			case u.ch <- sess:
			case <-ctx.Done():
				return nil
			}
			continue
		}
		u.mu.Unlock()
		_ = sess.Feed(payload)
	}
}

func (u *UDPListener) removeSession(key string) {
	u.mu.Lock()
	delete(u.sessions, key)
	u.mu.Unlock()
}
