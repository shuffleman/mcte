package client

import (
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shuffleman/mcte/pkg/protocol/java"
)

// javaBridgeConn 把客户端侧的 Java Play 阶段 Plugin Message 流抽象为 net.Conn。
//
// 上行（Write）：
//   - 若 Mimic == nil：按 32767 字节分帧封装为单一 channel 的 PluginMessage
//   - 若 Mimic != nil：按 Mimic.Pack 切片为多个 (channel, payload) chunk，
//     不同 channel 对应不同"玩家动作"（move/fly/chunk），让包大小分布像玩家行为
//
// 下行（Read）：循环读包；Mimic 模式下接受所有以 ChannelPrefix 开头的 channel
// （除 idle channel，idle 数据丢弃），KeepAlive 自动回包。
type javaBridgeConn struct {
	fr       *java.Framer
	raw      net.Conn
	channel  string // 兼容模式下的单 channel
	mimic    *MimicProfile
	proto    int32
	cbID     int32
	sbID     int32
	kaSBID   int32
	kaCBID   int32
	playSBKA int32

	readBufMu sync.Mutex
	readBuf   []byte

	// 心跳：上行有写时刷新 lastWrite；后台 goroutine 周期检查距上次写超
	// HeartbeatInterval 则发 idle 包
	lastWrite atomic.Int64
	stopHB    chan struct{}
	hbOnce    sync.Once

	closed sync.Once
}

func newJavaBridgeConn(fr *java.Framer, raw net.Conn, channel string, proto int32, mimic *MimicProfile) *javaBridgeConn {
	c := &javaBridgeConn{
		fr:       fr,
		raw:      raw,
		channel:  channel,
		mimic:    mimic,
		proto:    proto,
		cbID:     java.PacketID(proto, java.StatePlay, java.ClientBound, java.PPlayPluginMessageCB),
		sbID:     java.PacketID(proto, java.StatePlay, java.ServerBound, java.PPlayPluginMessageSB),
		kaCBID:   java.PacketID(proto, java.StatePlay, java.ClientBound, java.PKeepAliveCB),
		playSBKA: java.PacketID(proto, java.StatePlay, java.ServerBound, java.PKeepAliveSB),
		stopHB:   make(chan struct{}),
	}
	c.kaSBID = c.playSBKA
	c.lastWrite.Store(time.Now().UnixNano())
	return c
}

// startHeartbeat 启动 idle 心跳 goroutine。
func (c *javaBridgeConn) startHeartbeat() {
	c.hbOnce.Do(func() {
		go c.heartbeatLoop()
	})
}

func (c *javaBridgeConn) heartbeatLoop() {
	if c.mimic == nil || c.mimic.HeartbeatInterval <= 0 {
		return
	}
	interval := c.mimic.HeartbeatInterval
	checkEvery := interval / 2
	if checkEvery < 100*time.Millisecond {
		checkEvery = 100 * time.Millisecond
	}
	t := time.NewTicker(checkEvery)
	defer t.Stop()
	for {
		select {
		case <-c.stopHB:
			return
		case now := <-t.C:
			last := time.Unix(0, c.lastWrite.Load())
			if now.Sub(last) < interval {
				continue
			}
			hb := c.mimic.Heartbeat()
			pm := &java.PluginMessage{Channel: hb.Channel, Data: hb.Payload}
			if err := c.fr.WritePacket(java.Packet{ID: c.sbID, Payload: java.EncodePluginMessage(pm)}); err != nil {
				return
			}
			c.lastWrite.Store(now.UnixNano())
		}
	}
}

// Read 把 Plugin Message body 流式吐出。
func (c *javaBridgeConn) Read(p []byte) (int, error) {
	c.readBufMu.Lock()
	if len(c.readBuf) > 0 {
		n := copy(p, c.readBuf)
		c.readBuf = c.readBuf[n:]
		c.readBufMu.Unlock()
		return n, nil
	}
	c.readBufMu.Unlock()

	for {
		pkt, err := c.fr.ReadPacket()
		if err != nil {
			return 0, err
		}
		switch pkt.ID {
		case c.cbID:
			pm, err := java.DecodePluginMessage(pkt.Payload)
			if err != nil {
				return 0, err
			}
			if !c.matchChannel(pm.Channel) {
				continue
			}
			if c.mimic != nil && c.mimic.IsIdleChannel(pm.Channel) {
				continue
			}
			if len(pm.Data) == 0 {
				continue
			}
			n := copy(p, pm.Data)
			if n < len(pm.Data) {
				c.readBufMu.Lock()
				c.readBuf = append(c.readBuf, pm.Data[n:]...)
				c.readBufMu.Unlock()
			}
			return n, nil
		case c.kaCBID:
			ka, err := java.DecodeKeepAlive(pkt.Payload)
			if err != nil {
				return 0, err
			}
			payload := java.EncodeKeepAlive(&java.KeepAlive{ID: ka.ID})
			if werr := c.fr.WritePacket(java.Packet{ID: c.kaSBID, Payload: payload}); werr != nil {
				return 0, werr
			}
		default:
			// 其他 Play 包丢弃
		}
	}
}

// matchChannel：mimic 模式按前缀；否则严格相等。
func (c *javaBridgeConn) matchChannel(ch string) bool {
	if c.mimic != nil {
		return strings.HasPrefix(ch, c.mimic.ChannelPrefix)
	}
	return ch == c.channel
}

// Write 上行：按 Mimic 切片或单 channel 分帧。
func (c *javaBridgeConn) Write(p []byte) (int, error) {
	if c.sbID < 0 {
		return 0, errors.New("client: play plugin message sb id not found")
	}
	if len(p) == 0 {
		return 0, nil
	}
	if c.mimic == nil {
		return c.writeSingleChannel(p)
	}
	return c.writeMimic(p)
}

// writeSingleChannel 兼容模式：单 channel + 32767 分帧。
func (c *javaBridgeConn) writeSingleChannel(p []byte) (int, error) {
	written := 0
	total := len(p)
	for len(p) > 0 {
		chunk := p
		if len(chunk) > 32767 {
			chunk = chunk[:32767]
		}
		pm := &java.PluginMessage{Channel: c.channel, Data: chunk}
		if err := c.fr.WritePacket(java.Packet{ID: c.sbID, Payload: java.EncodePluginMessage(pm)}); err != nil {
			return written, err
		}
		written += len(chunk)
		p = p[len(chunk):]
	}
	c.lastWrite.Store(time.Now().UnixNano())
	_ = total
	return written, nil
}

// writeMimic 流量画像模式：按 size 选 channel + 多 chunk。
func (c *javaBridgeConn) writeMimic(p []byte) (int, error) {
	chunks := c.mimic.Pack(p)
	written := 0
	for _, ch := range chunks {
		pm := &java.PluginMessage{Channel: ch.Channel, Data: ch.Payload}
		if err := c.fr.WritePacket(java.Packet{ID: c.sbID, Payload: java.EncodePluginMessage(pm)}); err != nil {
			return written, err
		}
		written += len(ch.Payload)
	}
	c.lastWrite.Store(time.Now().UnixNano())
	return written, nil
}

func (c *javaBridgeConn) Close() error {
	var err error
	c.closed.Do(func() {
		close(c.stopHB)
		err = c.raw.Close()
	})
	return err
}

func (c *javaBridgeConn) LocalAddr() net.Addr                { return c.raw.LocalAddr() }
func (c *javaBridgeConn) RemoteAddr() net.Addr               { return c.raw.RemoteAddr() }
func (c *javaBridgeConn) SetDeadline(t time.Time) error      { return c.raw.SetDeadline(t) }
func (c *javaBridgeConn) SetReadDeadline(t time.Time) error  { return c.raw.SetReadDeadline(t) }
func (c *javaBridgeConn) SetWriteDeadline(t time.Time) error { return c.raw.SetWriteDeadline(t) }
