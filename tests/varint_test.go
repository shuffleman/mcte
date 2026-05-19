package tests

import (
	"bytes"
	"testing"

	"github.com/shuffleman/mcte/pkg/protocol/java"
)

func TestVarIntRoundtrip(t *testing.T) {
	cases := []int32{0, 1, 127, 128, 255, 2147483647, -1, -2147483648}
	for _, v := range cases {
		var buf bytes.Buffer
		if err := java.WriteVarInt(&buf, v); err != nil {
			t.Fatalf("write %d: %v", v, err)
		}
		got, _, err := java.ReadVarInt(&buf)
		if err != nil {
			t.Fatalf("read %d: %v", v, err)
		}
		if got != v {
			t.Fatalf("expected %d got %d", v, got)
		}
	}
}

func TestVarIntTooLong(t *testing.T) {
	buf := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	if _, _, err := java.ReadVarIntBytes(buf); err == nil {
		t.Fatalf("expected too long")
	}
}

func TestAppendVarIntZero(t *testing.T) {
	b := java.AppendVarInt(nil, 0)
	if len(b) != 1 || b[0] != 0 {
		t.Fatalf("expected [0], got %v", b)
	}
}
