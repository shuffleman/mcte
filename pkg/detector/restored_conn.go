package detector

import (
	"bytes"
	"io"
	"net"
	"time"
)

// RestoredConn 将已经 TeeReader 读出的字节与原始 conn 拼接，对外表现为完整 net.Conn。
// 调用方可在 Detector 判断后继续读到所有原始字节，不会丢失握手包。
type RestoredConn struct {
	consumed *bytes.Buffer
	raw      net.Conn
	reader   io.Reader
}

// NewRestoredConn 构造 RestoredConn。
//   consumed: TeeReader 已读取的字节
//   raw: 原始 conn
func NewRestoredConn(consumed []byte, raw net.Conn) *RestoredConn {
	buf := bytes.NewBuffer(consumed)
	return &RestoredConn{
		consumed: buf,
		raw:      raw,
		reader:   io.MultiReader(buf, raw),
	}
}

// Read 先消耗 buffer 中的字节，buffer 耗尽后无缝切换到原始 conn。
// 不允许提前返回 io.EOF。
func (c *RestoredConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

func (c *RestoredConn) Write(p []byte) (int, error)        { return c.raw.Write(p) }
func (c *RestoredConn) Close() error                       { return c.raw.Close() }
func (c *RestoredConn) LocalAddr() net.Addr                { return c.raw.LocalAddr() }
func (c *RestoredConn) RemoteAddr() net.Addr               { return c.raw.RemoteAddr() }
func (c *RestoredConn) SetDeadline(t time.Time) error      { return c.raw.SetDeadline(t) }
func (c *RestoredConn) SetReadDeadline(t time.Time) error  { return c.raw.SetReadDeadline(t) }
func (c *RestoredConn) SetWriteDeadline(t time.Time) error { return c.raw.SetWriteDeadline(t) }
