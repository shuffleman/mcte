package tests

import (
	"bytes"
	"testing"

	"github.com/shuffleman/mcte/pkg/protocol/bedrock"
)

func TestVarUint32Roundtrip(t *testing.T) {
	cases := []uint32{0, 1, 127, 128, 16383, 16384, 0x7FFFFFFF}
	for _, v := range cases {
		buf := bedrock.AppendVarUint32(nil, v)
		got, n, err := bedrock.ReadVarUint32(buf)
		if err != nil || got != v || n != len(buf) {
			t.Fatalf("varuint32 %d roundtrip got=%d n=%d err=%v", v, got, n, err)
		}
	}
}

func TestBatchAssembleRoundtrip(t *testing.T) {
	pkt := append(bedrock.PackHeader(bedrock.PacketPlayStatus), 0, 0, 0, 0)
	gp, err := bedrock.AssembleGamePacket(bedrock.CompZlib, [][]byte{pkt})
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	if gp[0] != bedrock.IDGamePacket {
		t.Fatalf("not game packet")
	}
	dec, err := bedrock.DecompressBatch(gp[1:])
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	pkts, err := bedrock.SplitBatchPackets(dec)
	if err != nil {
		t.Fatalf("split: %v", err)
	}
	if len(pkts) != 1 || !bytes.Equal(pkts[0], pkt) {
		t.Fatalf("packet mismatch")
	}
}

func TestSnappyBatch(t *testing.T) {
	pkt := []byte{0x02, 0x00, 0x00, 0x00, 0x00}
	gp, err := bedrock.AssembleGamePacket(bedrock.CompSnappy, [][]byte{pkt})
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	dec, err := bedrock.DecompressBatch(gp[1:])
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	pkts, _ := bedrock.SplitBatchPackets(dec)
	if !bytes.Equal(pkts[0], pkt) {
		t.Fatalf("snappy mismatch")
	}
}

func TestPlayStatusPayload(t *testing.T) {
	p := bedrock.EncodePlayStatusPayload(bedrock.PlayStatusLoginSuccess)
	if len(p) != 4 || p[3] != 0 {
		t.Fatalf("play status payload wrong: %v", p)
	}
}
