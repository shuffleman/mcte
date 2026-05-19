package tests

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/shuffleman/mcte/pkg/protocol/java"
)

// rwbuf 一对 io.ReadWriter；读写共用同一 bytes.Buffer。
type rwbuf struct{ *bytes.Buffer }

func (r *rwbuf) Read(p []byte) (int, error)  { return r.Buffer.Read(p) }
func (r *rwbuf) Write(p []byte) (int, error) { return r.Buffer.Write(p) }

func TestFramerRoundtrip(t *testing.T) {
	buf := &rwbuf{Buffer: &bytes.Buffer{}}
	fr := java.NewFramer(buf)

	payload := []byte("hello world")
	if err := fr.WritePacket(java.Packet{ID: 0x05, Payload: payload}); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := fr.ReadPacket()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got.ID != 5 {
		t.Fatalf("expected id 5 got %d", got.ID)
	}
	if !bytes.Equal(got.Payload, payload) {
		t.Fatalf("payload mismatch")
	}
}

func TestFramerTruncated(t *testing.T) {
	// 仅写一个 VarInt 长度前缀，后面无 body
	buf := &rwbuf{Buffer: bytes.NewBuffer([]byte{0x05})}
	fr := java.NewFramer(buf)
	_, err := fr.ReadPacket()
	if err == nil {
		t.Fatalf("expected error")
	}
	if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Logf("got error: %v (acceptable)", err)
	}
}

func TestFramerTooLarge(t *testing.T) {
	// 写一个声明 packetLen > MaxPacketSize 的 VarInt：5MB
	var hdr bytes.Buffer
	_ = java.WriteVarInt(&hdr, 5*1024*1024)
	buf := &rwbuf{Buffer: &hdr}
	fr := java.NewFramer(buf)
	if _, err := fr.ReadPacket(); !errors.Is(err, java.ErrPacketTooLarge) {
		t.Fatalf("expected ErrPacketTooLarge got %v", err)
	}
}
