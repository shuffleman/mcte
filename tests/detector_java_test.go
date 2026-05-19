package tests

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/shuffleman/mcte/pkg/auth"
	"github.com/shuffleman/mcte/pkg/detector"
	"github.com/shuffleman/mcte/pkg/protocol/java"
)

type pipeConn struct {
	r io.Reader
	w *bytes.Buffer
}

func (p *pipeConn) Read(b []byte) (int, error)         { return p.r.Read(b) }
func (p *pipeConn) Write(b []byte) (int, error)        { return p.w.Write(b) }
func (p *pipeConn) Close() error                       { return nil }
func (p *pipeConn) LocalAddr() net.Addr                { return nil }
func (p *pipeConn) RemoteAddr() net.Addr               { return nil }
func (p *pipeConn) SetDeadline(t time.Time) error      { return nil }
func (p *pipeConn) SetReadDeadline(t time.Time) error  { return nil }
func (p *pipeConn) SetWriteDeadline(t time.Time) error { return nil }

func buildHandshakeBytes(host string) []byte {
	hs := java.EncodeHandshake(&java.Handshake{
		ProtocolVersion: 769,
		ServerAddress:   host,
		ServerPort:      25565,
		NextState:       2,
	})
	body := []byte{0x00}
	body = append(body, hs...)
	out := java.AppendVarInt(nil, int32(len(body)))
	out = append(out, body...)
	return out
}

func TestDetectVanillaMC(t *testing.T) {
	raw := buildHandshakeBytes("example.com")
	conn := &pipeConn{r: bytes.NewReader(raw), w: &bytes.Buffer{}}
	v := auth.NewValidator()
	res, err := detector.DetectJava(conn, detector.DetectJavaOption{Validator: v})
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if res.Kind != detector.KindMC {
		t.Fatalf("expected MC kind, got %v", res.Kind)
	}
	got := make([]byte, len(raw))
	if _, err := io.ReadFull(res.Conn, got); err != nil {
		t.Fatalf("reread: %v", err)
	}
	if !bytes.Equal(got, raw) {
		t.Fatalf("restored conn mismatch")
	}
}

func TestDetectTunnel(t *testing.T) {
	v := auth.NewValidator()
	id := uuid.New()
	user := &auth.User{Name: "alice", UUID: id}
	_ = v.Add(user)
	raw := buildHandshakeBytes(detector.EncodeAddrWithUUID("example.com", id))
	conn := &pipeConn{r: bytes.NewReader(raw), w: &bytes.Buffer{}}
	res, err := detector.DetectJava(conn, detector.DetectJavaOption{Validator: v})
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if res.Kind != detector.KindTunnel {
		t.Fatalf("expected Tunnel got %v", res.Kind)
	}
	if res.User == nil || res.User.Name != "alice" {
		t.Fatalf("user mismatch: %v", res.User)
	}
	if res.Host != "example.com" {
		t.Fatalf("host mismatch: %q", res.Host)
	}
}

func TestDetectUnknownUUIDDowngrade(t *testing.T) {
	v := auth.NewValidator() // 空，所有 UUID 都查不到
	raw := buildHandshakeBytes(detector.EncodeAddrWithUUID("example.com", uuid.New()))
	conn := &pipeConn{r: bytes.NewReader(raw), w: &bytes.Buffer{}}
	res, err := detector.DetectJava(conn, detector.DetectJavaOption{Validator: v})
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if res.Kind != detector.KindMC {
		t.Fatalf("expected MC kind on unknown UUID, got %v", res.Kind)
	}
}

func TestDetectShortHandshake(t *testing.T) {
	conn := &pipeConn{r: bytes.NewReader([]byte{0x01}), w: &bytes.Buffer{}}
	v := auth.NewValidator()
	_, err := detector.DetectJava(conn, detector.DetectJavaOption{Validator: v})
	if err == nil {
		t.Fatalf("expected error on truncated input")
	}
	if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Logf("got: %v", err)
	}
}
