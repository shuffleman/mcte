package tests

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shuffleman/mcte/pkg/protocol/java"
)

// 验证去掉 framer.writeMu 之后，多 goroutine 并发 WritePacket 不会数据交错。
// net.Pipe 内部对 Write 有 mutex 保证原子性，足以模拟 *net.TCPConn 的 fd.writeLock。
func TestFramerConcurrentWriteNoInterleave(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()

	writer := java.NewFramer(a)
	reader := java.NewFramer(b)

	const goroutines = 32
	const perG = 50
	const total = goroutines * perG

	var wg sync.WaitGroup
	var wrErr atomic.Value
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				id := int32(g*perG + i)
				// 让 payload 体现包 id，便于校验完整性
				payload := make([]byte, 200)
				for k := range payload {
					payload[k] = byte(id) // 整包同一个字节
				}
				if err := writer.WritePacket(java.Packet{ID: id, Payload: payload}); err != nil {
					wrErr.Store(err)
					return
				}
			}
		}(g)
	}

	// 读端：并发读 total 个包
	done := make(chan error, 1)
	go func() {
		seen := make(map[int32]bool, total)
		for n := 0; n < total; n++ {
			pkt, err := reader.ReadPacket()
			if err != nil {
				done <- err
				return
			}
			if len(pkt.Payload) != 200 {
				done <- &payloadError{id: pkt.ID, gotLen: len(pkt.Payload)}
				return
			}
			expect := byte(pkt.ID)
			for _, b := range pkt.Payload {
				if b != expect {
					done <- &payloadError{id: pkt.ID, gotLen: len(pkt.Payload), corrupt: true}
					return
				}
			}
			if seen[pkt.ID] {
				done <- &dupError{id: pkt.ID}
				return
			}
			seen[pkt.ID] = true
		}
		if len(seen) != total {
			done <- &countError{got: len(seen), want: total}
			return
		}
		done <- nil
	}()

	wg.Wait()
	if e := wrErr.Load(); e != nil {
		t.Fatalf("write error: %v", e)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("read: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("read did not finish within 10s")
	}
}

type payloadError struct {
	id      int32
	gotLen  int
	corrupt bool
}

func (e *payloadError) Error() string {
	if e.corrupt {
		return "payload corrupted (byte mismatch)"
	}
	return "payload length wrong"
}

type dupError struct{ id int32 }

func (e *dupError) Error() string { return "duplicate packet id" }

type countError struct{ got, want int }

func (e *countError) Error() string { return "packet count mismatch" }
