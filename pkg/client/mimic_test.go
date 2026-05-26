package client

import (
	"bytes"
	"testing"
)

// stripFrame 复刻服务端 tunnel.stripFramePrefix 的格式：[1B prefixLen][prefixLen][data]。
func stripFrame(d []byte) []byte {
	if len(d) == 0 {
		return nil
	}
	l := int(d[0])
	if 1+l > len(d) {
		return nil
	}
	return d[1+l:]
}

// Pack 出的小帧剥离帧头后应当无损拼回原始字节流，且每帧数据切片不超过 C2SMaxFrame。
func TestPackSmallFramesRoundTrip(t *testing.T) {
	p := DefaultProfile()
	p.normalize()
	orig := make([]byte, 5000)
	for i := range orig {
		orig[i] = byte(i)
	}
	chunks := p.Pack(orig)
	if len(chunks) == 0 {
		t.Fatal("Pack returned no chunks")
	}
	moveCh := p.ChannelPrefix + p.MoveSuffix
	var reassembled bytes.Buffer
	for i, ch := range chunks {
		if ch.Channel != moveCh {
			t.Fatalf("chunk %d channel=%q want move channel %q", i, ch.Channel, moveCh)
		}
		data := stripFrame(ch.Payload)
		if data == nil {
			t.Fatalf("chunk %d frame prefix strip failed (payload len=%d)", i, len(ch.Payload))
		}
		if len(data) > p.C2SMaxFrame {
			t.Fatalf("chunk %d data slice %d > C2SMaxFrame %d", i, len(data), p.C2SMaxFrame)
		}
		reassembled.Write(data)
	}
	if !bytes.Equal(reassembled.Bytes(), orig) {
		t.Fatalf("reassembled %d bytes != original %d bytes", reassembled.Len(), len(orig))
	}
}

// 启用熵前缀后仍应无损 round-trip，且帧头声明的 prefixLen 在配置范围内。
func TestPackEntropyPrefixRoundTrip(t *testing.T) {
	p := DefaultProfile()
	p.EntropyPrefixMin = 8
	p.EntropyPrefixMax = 16
	p.normalize()
	orig := bytes.Repeat([]byte("hello-mcte-"), 200)
	chunks := p.Pack(orig)
	var reassembled bytes.Buffer
	for i, ch := range chunks {
		if len(ch.Payload) == 0 {
			t.Fatalf("chunk %d empty payload", i)
		}
		prefixLen := int(ch.Payload[0])
		if prefixLen < p.EntropyPrefixMin || prefixLen > p.EntropyPrefixMax {
			t.Fatalf("chunk %d prefixLen %d out of [%d,%d]", i, prefixLen, p.EntropyPrefixMin, p.EntropyPrefixMax)
		}
		reassembled.Write(stripFrame(ch.Payload))
	}
	if !bytes.Equal(reassembled.Bytes(), orig) {
		t.Fatal("entropy-prefixed reassembly mismatch")
	}
}

// 小数据（< 一帧）也应正确打包。
func TestPackTinyData(t *testing.T) {
	p := DefaultProfile()
	chunks := p.Pack([]byte("hi"))
	if len(chunks) != 1 {
		t.Fatalf("want 1 chunk, got %d", len(chunks))
	}
	if got := stripFrame(chunks[0].Payload); string(got) != "hi" {
		t.Fatalf("round-trip got %q want hi", got)
	}
}

// MovePacket 大小应在 [MoveSizeMin, MoveSizeMax]。
func TestMovePacketSize(t *testing.T) {
	p := DefaultProfile()
	p.normalize()
	for i := 0; i < 200; i++ {
		n := len(p.MovePacket())
		if n < p.MoveSizeMin || n > p.MoveSizeMax {
			t.Fatalf("MovePacket size %d out of [%d,%d]", n, p.MoveSizeMin, p.MoveSizeMax)
		}
	}
}
