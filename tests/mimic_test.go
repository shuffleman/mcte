package tests

import (
	"testing"

	"github.com/shuffleman/mcte/pkg/client"
)

// 不同 size 输入 → 期望对应 channel。
func TestMimicChannelByMessageSize(t *testing.T) {
	p := client.DefaultProfile()
	cases := []struct {
		size     int
		wantChan string
	}{
		{1, p.Channel(client.ActionMove)},
		{32, p.Channel(client.ActionMove)},
		{33, p.Channel(client.ActionFly)},
		{256, p.Channel(client.ActionFly)},
		{257, p.Channel(client.ActionChunk)},
		{4000, p.Channel(client.ActionChunk)},
	}
	for _, tc := range cases {
		data := make([]byte, tc.size)
		chunks := p.Pack(data)
		if len(chunks) == 0 {
			t.Fatalf("size=%d produced no chunks", tc.size)
		}
		if chunks[0].Channel != tc.wantChan {
			t.Fatalf("size=%d first channel=%q want %q", tc.size, chunks[0].Channel, tc.wantChan)
		}
	}
}

// 大包按 ChunkSize 切片，所有切片都是 ChunkSuffix channel。
func TestMimicChunkSplit(t *testing.T) {
	p := &client.MimicProfile{
		ChannelPrefix: "mcte:",
		MoveSuffix:    "m",
		FlySuffix:     "f",
		ChunkSuffix:   "c",
		IdleSuffix:    "i",
		MoveThreshold: 32,
		FlyThreshold:  256,
		ChunkSize:     1000,
	}
	data := make([]byte, 3500)
	chunks := p.Pack(data)
	if len(chunks) != 4 {
		t.Fatalf("3500/1000 expect 4 chunks, got %d", len(chunks))
	}
	for i, ch := range chunks {
		if ch.Action != client.ActionChunk {
			t.Fatalf("chunk %d action=%v want ActionChunk", i, ch.Action)
		}
		if ch.Channel != "mcte:c" {
			t.Fatalf("chunk %d channel=%q", i, ch.Channel)
		}
	}
	if chunks[0].Payload[0] != 0 || len(chunks[3].Payload) != 500 {
		t.Fatalf("last chunk size = %d want 500", len(chunks[3].Payload))
	}
}

// Pack 必须保持字节顺序和完整性：拼回应等于原 data。
func TestMimicRoundtrip(t *testing.T) {
	p := client.DefaultProfile()
	data := make([]byte, 5000)
	for i := range data {
		data[i] = byte(i % 251)
	}
	chunks := p.Pack(data)
	out := make([]byte, 0, len(data))
	for _, ch := range chunks {
		out = append(out, ch.Payload...)
	}
	if len(out) != len(data) {
		t.Fatalf("roundtrip len mismatch: %d vs %d", len(out), len(data))
	}
	for i := range data {
		if out[i] != data[i] {
			t.Fatalf("byte %d mismatch", i)
		}
	}
}

// Heartbeat 空包，channel 是 idle channel。
func TestMimicHeartbeat(t *testing.T) {
	p := client.DefaultProfile()
	hb := p.Heartbeat()
	if hb.Action != client.ActionIdle {
		t.Fatalf("hb action = %v", hb.Action)
	}
	if len(hb.Payload) != 0 {
		t.Fatalf("hb payload should be empty")
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

// 空数据 Pack 返回 nil（不能产生任何包，让 idle 心跳负责）。
func TestMimicPackEmpty(t *testing.T) {
	p := client.DefaultProfile()
	if chunks := p.Pack(nil); chunks != nil {
		t.Fatalf("empty pack expected nil, got %v", chunks)
	}
}
