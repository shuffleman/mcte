package tests

import (
	"bytes"
	"testing"

	"github.com/shuffleman/mcte/pkg/client"
)

// stripFrame 复刻服务端 tunnel.stripFramePrefix：[1B prefixLen][prefixLen][data]。
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

// 新设计：所有 C→S 数据帧都走 move channel（funnel，抬高 top1 占比，匹配 MC）。
func TestMimicAllFramesMoveChannel(t *testing.T) {
	p := client.DefaultProfile()
	moveCh := p.Channel(client.ActionMove)
	for _, size := range []int{1, 32, 256, 4000} {
		data := make([]byte, size)
		chunks := p.Pack(data)
		if len(chunks) == 0 {
			t.Fatalf("size=%d produced no chunks", size)
		}
		for i, ch := range chunks {
			if ch.Channel != moveCh {
				t.Fatalf("size=%d chunk %d channel=%q want move %q", size, i, ch.Channel, moveCh)
			}
		}
	}
}

// 新设计：C→S 拆成小帧，每帧数据切片 ≤ C2SMaxFrame（默认 64），匹配 MC "C→S 95%+ < 100B"。
func TestMimicSmallFrames(t *testing.T) {
	p := client.DefaultProfile()
	data := make([]byte, 3500)
	chunks := p.Pack(data)
	if len(chunks) < 3500/p.C2SMaxFrame {
		t.Fatalf("3500 bytes only produced %d small frames", len(chunks))
	}
	for i, ch := range chunks {
		body := stripFrame(ch.Payload)
		if body == nil {
			t.Fatalf("chunk %d strip failed", i)
		}
		if len(body) > p.C2SMaxFrame {
			t.Fatalf("chunk %d body %d > C2SMaxFrame %d", i, len(body), p.C2SMaxFrame)
		}
	}
}

// Pack 剥离帧头后必须无损拼回原始字节流。
func TestMimicRoundtrip(t *testing.T) {
	p := client.DefaultProfile()
	data := make([]byte, 5000)
	for i := range data {
		data[i] = byte(i % 251)
	}
	chunks := p.Pack(data)
	var out bytes.Buffer
	for _, ch := range chunks {
		out.Write(stripFrame(ch.Payload))
	}
	if !bytes.Equal(out.Bytes(), data) {
		t.Fatalf("roundtrip mismatch: %d vs %d", out.Len(), len(data))
	}
}

// 新设计：Heartbeat 是低熵移动包（非空），channel 为 idle channel（服务端丢弃）。
func TestMimicHeartbeat(t *testing.T) {
	p := client.DefaultProfile()
	hb := p.Heartbeat()
	if hb.Action != client.ActionIdle {
		t.Fatalf("hb action = %v", hb.Action)
	}
	if len(hb.Payload) == 0 {
		t.Fatalf("hb payload should be a non-empty move packet")
	}
	if !p.IsIdleChannel(hb.Channel) {
		t.Fatalf("hb channel %q not recognized as idle", hb.Channel)
	}
}

// IsTunnelChannel 前缀匹配。
func TestMimicTunnelChannelMatch(t *testing.T) {
	p := client.DefaultProfile()
	for _, ch := range []string{"mcte:m", "mcte:f", "mcte:c", "mcte:i", "mcte:custom"} {
		if !p.IsTunnelChannel(ch) {
			t.Fatalf("expected tunnel: %q", ch)
		}
	}
	for _, ch := range []string{"minecraft:brand", "other:data", "mctex:m", ""} {
		if p.IsTunnelChannel(ch) {
			t.Fatalf("unexpected tunnel: %q", ch)
		}
	}
}

// 空数据 Pack 返回 nil（由 idle 移动流负责保活）。
func TestMimicPackEmpty(t *testing.T) {
	p := client.DefaultProfile()
	if chunks := p.Pack(nil); chunks != nil {
		t.Fatalf("empty pack expected nil, got %v", chunks)
	}
}
