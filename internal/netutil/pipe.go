// Package netutil 提供网络辅助工具。
package netutil

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
)

// Pipe 在两个 net.Conn 之间做双向 io.Copy；任一方向出错或 ctx 取消时同时关闭两端。
// 返回首个错误（io.EOF/closed/context.Canceled 等被视为正常关闭）。
func Pipe(ctx context.Context, a, b net.Conn) error {
	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		first  error
		closed bool
	)

	closeBoth := func() {
		mu.Lock()
		defer mu.Unlock()
		if closed {
			return
		}
		closed = true
		_ = a.Close()
		_ = b.Close()
	}

	record := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if first == nil && err != nil && !isNormalClose(err) {
			first = err
		}
	}

	copyOne := func(dst, src net.Conn) {
		defer wg.Done()
		_, err := io.Copy(dst, src)
		record(err)
		closeBoth()
	}

	wg.Add(2)
	go copyOne(a, b)
	go copyOne(b, a)

	// ctx 取消时主动关闭，让两个 Copy 退出
	cancelDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			closeBoth()
		case <-cancelDone:
		}
	}()

	wg.Wait()
	close(cancelDone)
	return first
}

func isNormalClose(err error) bool {
	if err == nil || errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || errors.Is(err, context.Canceled) {
		return true
	}
	return false
}
