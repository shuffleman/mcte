package tests

import (
	"bytes"
	"net"
	"testing"

	"github.com/shuffleman/mcte/pkg/protocol/bedrock/raknet"
)

func TestRakNetAddrRoundtrip(t *testing.T) {
	a := &net.UDPAddr{IP: net.IPv4(192, 168, 1, 1), Port: 19132}
	buf := raknet.EncodeAddress(nil, a)
	got, n, err := raknet.DecodeAddress(buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if n != len(buf) {
		t.Fatalf("n=%d want %d", n, len(buf))
	}
	if !got.IP.Equal(a.IP) || got.Port != a.Port {
		t.Fatalf("addr mismatch: got %v want %v", got, a)
	}
}

func TestOfflinePingPong(t *testing.T) {
	in := make([]byte, 0, 33)
	// IDUnconnectedPing(1) + 8B time + 16B magic + 8B guid
	in = append(in, raknet.IDUnconnectedPing)
	in = append(in, []byte{0, 0, 0, 0, 0, 0, 0, 42}...)
	in = append(in, raknet.Magic[:]...)
	in = append(in, []byte{0, 0, 0, 0, 0, 0, 0, 1}...)
	p, err := raknet.ParseUnconnectedPing(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if p.PingTime != 42 {
		t.Fatalf("ping time = %d", p.PingTime)
	}
	pong := raknet.BuildUnconnectedPong(42, 99, "MCPE;X;800;1.21.4;0;1;99;;;;19132;19133;")
	if pong[0] != raknet.IDUnconnectedPong {
		t.Fatalf("bad pong id")
	}
	if !bytes.Contains(pong, raknet.Magic[:]) {
		t.Fatalf("magic not embedded")
	}
}

func TestOpenConnectionReply1Build(t *testing.T) {
	b := raknet.BuildOpenConnectionReply1(123, 1400)
	if b[0] != raknet.IDOpenConnectionReply1 {
		t.Fatalf("bad id")
	}
	if !bytes.Equal(b[1:17], raknet.Magic[:]) {
		t.Fatalf("bad magic")
	}
}

func TestEncapsulatedRoundtrip(t *testing.T) {
	in := []raknet.EncapsulatedPacket{
		{
			Reliability: raknet.RelReliableOrdered,
			MsgIndex:    1,
			OrderIndex:  1,
			OrderChan:   0,
			Body:        []byte("hello"),
		},
	}
	enc := raknet.EncodeEncapsulated(in)
	out, err := raknet.DecodeEncapsulated(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out) != 1 || string(out[0].Body) != "hello" {
		t.Fatalf("roundtrip mismatch: %+v", out)
	}
}

func TestAckRangeCompact(t *testing.T) {
	seqs := []uint32{1, 2, 3, 5, 6, 10}
	rec := raknet.CompactAckRanges(seqs)
	if len(rec) != 3 {
		t.Fatalf("want 3 ranges got %d (%+v)", len(rec), rec)
	}
	enc := raknet.EncodeAck(rec)
	dec, err := raknet.DecodeAck(enc)
	if err != nil {
		t.Fatalf("decode ack: %v", err)
	}
	if len(dec) != len(rec) {
		t.Fatalf("ack mismatch")
	}
}
