package tests

import (
	"testing"

	"github.com/google/uuid"

	"github.com/shuffleman/mcte/pkg/auth"
	"github.com/shuffleman/mcte/pkg/detector"
)

func TestExtractUUID(t *testing.T) {
	id := uuid.New()
	addr := detector.EncodeAddrWithUUID("example.com", id)
	host, got := detector.ExtractUUIDFromAddr(addr)
	if host != "example.com" {
		t.Fatalf("host=%q", host)
	}
	if got != id {
		t.Fatalf("uuid mismatch: %v vs %v", got, id)
	}
}

func TestExtractUUIDPlainHost(t *testing.T) {
	host, id := detector.ExtractUUIDFromAddr("example.com")
	if host != "example.com" || id != uuid.Nil {
		t.Fatalf("plain host should produce nil uuid: %v %v", host, id)
	}
}

func TestExtractUUIDInvalidTail(t *testing.T) {
	host, id := detector.ExtractUUIDFromAddr("example.com\x00not-a-uuid")
	if host != "example.com" || id != uuid.Nil {
		t.Fatalf("invalid uuid should fall back to nil: host=%q id=%v", host, id)
	}
}

func TestValidatorLookup(t *testing.T) {
	v := auth.NewValidator()
	id := uuid.New()
	u := &auth.User{Name: "alice", UUID: id}
	if err := v.Add(u); err != nil {
		t.Fatalf("add: %v", err)
	}
	if got := v.Lookup(id); got == nil || got.Name != "alice" {
		t.Fatalf("lookup mismatch: %v", got)
	}
	if got := v.Lookup(uuid.New()); got != nil {
		t.Fatalf("unknown uuid should return nil")
	}
}

func TestValidatorDuplicateRejected(t *testing.T) {
	v := auth.NewValidator()
	id := uuid.New()
	if err := v.Add(&auth.User{UUID: id}); err != nil {
		t.Fatalf("first add: %v", err)
	}
	if err := v.Add(&auth.User{UUID: id}); err == nil {
		t.Fatalf("expected duplicate error")
	}
}
