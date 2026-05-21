package client

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/shuffleman/mcte/internal/debug"
	"github.com/shuffleman/mcte/pkg/protocol/bedrock"
	"github.com/shuffleman/mcte/pkg/protocol/bedrock/raknet"
)

// bedrockBridgeConn 把 Bedrock 隧道的应用层 packet 流抽象为 net.Conn。
// 上行：每个 Write 调用打包成一个 PacketTunnelData 发送。
// 下行：拆出 PacketTunnelData 的 payload。
type bedrockBridgeConn struct {
	sess   *raknet.Session
	ctx    context.Context
	cancel context.CancelFunc

	readBufMu  sync.Mutex
	readBuf    []byte
	closed     sync.Once
	writeClosed sync.Once
	writeStopped bool
}

func newBedrockBridgeConn(parent context.Context, sess *raknet.Session) *bedrockBridgeConn {
	ctx, cancel := context.WithCancel(parent)
	return &bedrockBridgeConn{sess: sess, ctx: ctx, cancel: cancel}
}

func (b *bedrockBridgeConn) Read(p []byte) (int, error) {
	b.readBufMu.Lock()
	if len(b.readBuf) > 0 {
		n := copy(p, b.readBuf)
		b.readBuf = b.readBuf[n:]
		b.readBufMu.Unlock()
		return n, nil
	}
	b.readBufMu.Unlock()

	for {
		body, err := b.sess.ReadApp(b.ctx)
		if err != nil {
			return 0, err
		}
		if len(body) == 0 || body[0] != bedrock.IDGamePacket {
			continue
		}
		decompressed, err := bedrock.DecompressBatch(body[1:])
		if err != nil {
			return 0, err
		}
		packets, err := bedrock.SplitBatchPackets(decompressed)
		if err != nil {
			return 0, err
		}
		// 把同一 batch 里所有 PacketTunnelData 的 payload 顺序累积到 collected。
		// 之前实现遇到第一个就 return，会丢同 batch 后续 TunnelData → TLS 等上层
		// 协议看到的字节流不完整或乱序 → 解密失败 / framing 错。
		var collected []byte
		for _, pkt := range packets {
			hdr, m, err := bedrock.ReadVarUint32(pkt)
			if err != nil {
				continue
			}
			if hdr&0x3FF != bedrock.PacketTunnelData {
				continue
			}
			data := pkt[m:]
			if len(data) == 0 {
				continue
			}
			collected = append(collected, data...)
		}
		if len(collected) == 0 {
			continue
		}
		n := copy(p, collected)
		if n < len(collected) {
			b.readBufMu.Lock()
			b.readBuf = append(b.readBuf, collected[n:]...)
			b.readBufMu.Unlock()
		}
		return n, nil
	}
}

func (b *bedrockBridgeConn) Write(p []byte) (int, error) {
	if b.writeStopped {
		return 0, net.ErrClosed
	}
	gp, err := bedrock.AssembleSingle(bedrock.CompZlib, bedrock.PacketTunnelData, p)
	if err != nil {
		return 0, err
	}
	if err := b.sess.WriteAppCtx(b.ctx, gp); err != nil {
		return 0, err
	}
	return len(p), nil
}

// CloseWrite 实现 sing-box / sing/common/network 的 N.WriteCloser 接口（也是 net.TCPConn
// 的方法）。半关闭写方向但保留读 — sing-box 的 ConnectionCopy 上行完成后会优先调用此方法
// 而不是 Close()，避免关闭整个 session 导致下行未传完数据被切断。
//
// 注意：MCTE 协议层没有"半关闭"原语 — RakNet 双向是同一个 session。这里只在本端阻断 Write，
// 不告诉对端"我不再写了"。对端仍可继续发数据，本端 Read 仍然有效。Close() 才发 DC。
func (b *bedrockBridgeConn) CloseWrite() error {
	b.writeClosed.Do(func() {
		b.writeStopped = true
		if debug.Enabled() {
			debug.Logf("bedrockBridgeConn.CloseWrite called — write half-closed, read still alive")
		}
	})
	return nil
}

func (b *bedrockBridgeConn) Close() error {
	var err error
	b.closed.Do(func() {
		b.cancel()
		err = b.sess.Close()
	})
	return err
}

func (b *bedrockBridgeConn) LocalAddr() net.Addr  { return b.sess.Remote() }
func (b *bedrockBridgeConn) RemoteAddr() net.Addr { return b.sess.Remote() }

func (b *bedrockBridgeConn) SetDeadline(t time.Time) error      { return errors.New("not supported") }
func (b *bedrockBridgeConn) SetReadDeadline(t time.Time) error  { return errors.New("not supported") }
func (b *bedrockBridgeConn) SetWriteDeadline(t time.Time) error { return errors.New("not supported") }
