package memory

import (
	"runtime"
	"sync/atomic"
)

// Ring 是单生产者单消费者无锁环形缓冲。
// 容量必须是 2 的幂次，便于使用 & mask 替代取模。
// 专用于 RakNet UDP 高频收包路径。
type Ring[T any] struct {
	buf  []T
	mask uint64
	// 64 字节填充，避免 head/tail 伪共享。
	_    [56]byte
	head atomic.Uint64
	_    [56]byte
	tail atomic.Uint64
	_    [56]byte
}

// NewRing 构造容量为 size 的环（size 必须 >= 2 且是 2 的幂次）。
func NewRing[T any](size int) *Ring[T] {
	if size < 2 || size&(size-1) != 0 {
		panic("ring size must be a power of two and >= 2")
	}
	return &Ring[T]{
		buf:  make([]T, size),
		mask: uint64(size - 1),
	}
}

// Push 由生产者调用；队列满返回 false。
func (r *Ring[T]) Push(v T) bool {
	h := r.head.Load()
	t := r.tail.Load()
	if h-t == uint64(len(r.buf)) {
		return false
	}
	r.buf[h&r.mask] = v
	r.head.Store(h + 1)
	return true
}

// Pop 由消费者调用；队列空返回零值与 false。
func (r *Ring[T]) Pop() (T, bool) {
	t := r.tail.Load()
	h := r.head.Load()
	var zero T
	if h == t {
		return zero, false
	}
	v := r.buf[t&r.mask]
	r.buf[t&r.mask] = zero
	r.tail.Store(t + 1)
	return v, true
}

// SpinPop 自旋拉取，自旋失败返回零值与 false；适合极低延迟场景。
func (r *Ring[T]) SpinPop(spins int) (T, bool) {
	for i := 0; i < spins; i++ {
		if v, ok := r.Pop(); ok {
			return v, true
		}
		runtime.Gosched()
	}
	var zero T
	return zero, false
}

// Len 返回当前元素数量（仅供观察，非强一致）。
func (r *Ring[T]) Len() int {
	return int(r.head.Load() - r.tail.Load())
}
