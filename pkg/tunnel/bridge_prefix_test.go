package tunnel

import (
	"bytes"
	"testing"
)

func TestStripFramePrefix(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
		want []byte
	}{
		{"zero prefix", []byte{0x00, 'a', 'b', 'c'}, []byte("abc")},
		{"with prefix", append([]byte{0x03, 0x00, 0x00, 0x0F}, []byte("data")...), []byte("data")},
		{"empty data after prefix", []byte{0x02, 0x00, 0x00}, []byte{}},
		{"empty input", []byte{}, nil},
		{"malformed: prefix exceeds buf", []byte{0x05, 0x00}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := stripFramePrefix(c.in)
			if c.want == nil {
				if got != nil {
					t.Fatalf("want nil got %v", got)
				}
				return
			}
			if !bytes.Equal(got, c.want) {
				t.Fatalf("got %q want %q", got, c.want)
			}
		})
	}
}
