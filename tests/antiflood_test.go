package tests

import (
	"testing"
	"time"

	"github.com/shuffleman/mcte/pkg/plugin/builtin/ratelimit"
	"github.com/shuffleman/mcte/pkg/protocol/bedrock/raknet"
)

// FragmentAssembler 拒绝超大 total。
func TestFragmentAssemblerMaxTotalRejected(t *testing.T) {
	a := raknet.NewFragmentAssembler(0)
	_, _, err := a.Add(1, 0, 65535, []byte{1})
	if err == nil {
		t.Fatalf("expected reject for excessive total")
	}
	_, dropMaxGroups, _, dropMaxTotal, _ := a.Stats()
	if dropMaxTotal == 0 || dropMaxGroups != 0 {
		t.Fatalf("expected dropMaxTotal>0, got dropMaxTotal=%d dropMaxGroups=%d", dropMaxTotal, dropMaxGroups)
	}
}

// FragmentAssembler LRU 淘汰过量 group。
func TestFragmentAssemblerLRUEvict(t *testing.T) {
	a := raknet.NewFragmentAssembler(0)
	// 默认 maxGroups=64；制造 100 个不同 compoundID 各占一片
	for i := 0; i < 100; i++ {
		_, _, err := a.Add(uint16(i), 0, 4, []byte{1})
		if err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
	}
	size, dropMaxGroups, _, _, _ := a.Stats()
	if size > 64 {
		t.Fatalf("expected size<=64, got %d", size)
	}
	if dropMaxGroups == 0 {
		t.Fatalf("expected at least one LRU eviction")
	}
}

// FragmentAssembler TTL 清理未完成 group。
func TestFragmentAssemblerSweepExpired(t *testing.T) {
	a := raknet.NewFragmentAssembler(0)
	_, _, _ = a.Add(7, 0, 4, []byte{1})
	// 模拟时间前移 10 秒
	a.SweepExpired(time.Now().Add(10 * time.Second))
	size, _, dropTimeout, _, _ := a.Stats()
	if size != 0 {
		t.Fatalf("expected empty after sweep, got %d", size)
	}
	if dropTimeout == 0 {
		t.Fatalf("expected timeout drop counted")
	}
}

// FragmentAssembler 拼装完整数据。
func TestFragmentAssemblerCompose(t *testing.T) {
	a := raknet.NewFragmentAssembler(0)
	if _, done, _ := a.Add(9, 0, 3, []byte("hel")); done {
		t.Fatalf("not done yet")
	}
	if _, done, _ := a.Add(9, 2, 3, []byte("lo!")); done {
		t.Fatalf("not done yet")
	}
	out, done, err := a.Add(9, 1, 3, []byte("lo-"))
	if err != nil || !done {
		t.Fatalf("expected done: err=%v done=%v", err, done)
	}
	if string(out) != "hello-lo!" {
		t.Fatalf("bad compose: %q", out)
	}
}

// Ratelimit LRU 淘汰过量 IP。
func TestRatelimitLRUEvict(t *testing.T) {
	p := ratelimit.NewWithOptions(10, 5, 32, time.Hour)
	for i := 0; i < 100; i++ {
		// 每个 IP 不同
		ip := "10.0." + itoaTest(i/256) + "." + itoaTest(i%256)
		_ = p.Allow(ip)
	}
	size, evicted := p.Stats()
	if size > 32 {
		t.Fatalf("expected size<=32, got %d", size)
	}
	if evicted == 0 {
		t.Fatalf("expected eviction count > 0")
	}
}

func itoaTest(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [4]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
