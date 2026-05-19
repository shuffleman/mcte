// Package memory 提供分级字节池与无锁环形缓冲。
package memory

import (
	"math/bits"
	"sync"
)

// 分级字节池：8 级，64B 到 8KB（2^6 .. 2^13）。
// 超出最大级别的请求直接 make，不入池。
const (
	minClassShift = 6  // 64B
	maxClassShift = 13 // 8KB
	classCount    = maxClassShift - minClassShift + 1
)

var pools [classCount]sync.Pool

func init() {
	for i := range pools {
		size := 1 << (minClassShift + i)
		pools[i].New = func() any {
			b := make([]byte, size)
			return &b
		}
	}
}

// classFor 返回大于等于 n 的最小池索引；返回 -1 表示超过最大级别。
func classFor(n int) int {
	if n <= 0 {
		return 0
	}
	if n > 1<<maxClassShift {
		return -1
	}
	shift := bits.Len(uint(n - 1))
	if shift < minClassShift {
		shift = minClassShift
	}
	return shift - minClassShift
}

// Get 取一块容量 >= size 的 []byte，长度被设为 size。
func Get(size int) []byte {
	idx := classFor(size)
	if idx < 0 {
		return make([]byte, size)
	}
	p := pools[idx].Get().(*[]byte)
	return (*p)[:size]
}

// Put 归还由 Get 取出的 []byte。容量不在池范围内的会被丢弃。
func Put(b []byte) {
	if b == nil {
		return
	}
	c := cap(b)
	if c < 1<<minClassShift || c > 1<<maxClassShift {
		return
	}
	if bits.OnesCount(uint(c)) != 1 {
		return // 非 2 的幂次，非池内对象
	}
	idx := bits.TrailingZeros(uint(c)) - minClassShift
	if idx < 0 || idx >= classCount {
		return
	}
	b = b[:c]
	pools[idx].Put(&b)
}
