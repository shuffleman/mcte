package client

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

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

	readBufMu sync.Mutex
	readBuf   []byte
	closed    sync.Once
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
			n := copy(p, data)
			if n < len(data) {
				b.readBufMu.Lock()
				b.readBuf = append(b.readBuf, data[n:]...)
				b.readBufMu.Unlock()
			}
			return n, nil
		}
	}
}

func (b *bedrockBridgeConn) Write(p []byte) (int, error) {
	gp, err := bedrock.AssembleSingle(bedrock.CompZlib, bedrock.PacketTunnelData, p)
	if err != nil {
		return 0, err
	}
	if err := b.sess.WriteAppCtx(b.ctx, gp); err != nil {
		return 0, err
	}
	return len(p), nil
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
