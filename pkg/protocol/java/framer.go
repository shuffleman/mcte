package java

import (
	"bufio"
	"compress/zlib"
	"io"
)

// MaxPacketSize 单包最大长度（含头与负载）。
const MaxPacketSize = 2 * 1024 * 1024 // 2MB

// Framer 读写 MC 的 VarInt 长度前缀帧；可选 zlib 压缩与 AES-CFB8 加密。
//
// 写路径设计（vectorised / 单次 Write）：
//   每个 WritePacket 调用先在本地组装出完整的 datagram 字节 [VarInt(len)|payload]，
//   然后一次性调用底层 io.Writer.Write 发送。这样:
//     - 不需要 framer 层 mutex；
//     - 多 goroutine 并发写时由 *net.TCPConn 内部 fd.writeLock 保证不数据交错；
//     - KeepAlive 与 bridge 下行 goroutine 可真正并发。
//
// 网络字节布局（未压缩）：
//   VarInt(packetLen) | VarInt(packetID) + payload
//
// 启用压缩后（packetLen = dataLen 字段长度 + 数据长度）：
//   VarInt(packetLen) | VarInt(dataLen) | (zlib 压缩或明文) [VarInt(packetID) + payload]
//   dataLen=0 表示未压缩，>0 表示压缩前长度。
type Framer struct {
	r          *bufio.Reader
	w          io.Writer
	compThresh int // < 0 未启用压缩；>= 0 大于等于该长度时启用 zlib
}

// NewFramer 构造 Framer。底层 conn 可在外部包裹 AES-CFB8 stream。
func NewFramer(rw io.ReadWriter) *Framer {
	return &Framer{
		r:          bufio.NewReader(rw),
		w:          rw,
		compThresh: -1,
	}
}

// EnableCompression 启用压缩（thresh >= 0）。
func (f *Framer) EnableCompression(thresh int) {
	f.compThresh = thresh
}

// ReadPacket 读一个完整包。
func (f *Framer) ReadPacket() (Packet, error) {
	pktLen, _, err := readVarIntFromReader(f.r)
	if err != nil {
		return Packet{}, err
	}
	if pktLen <= 0 || int(pktLen) > MaxPacketSize {
		return Packet{}, ErrPacketTooLarge
	}
	body := make([]byte, pktLen)
	if _, err := io.ReadFull(f.r, body); err != nil {
		return Packet{}, err
	}

	if f.compThresh >= 0 {
		dataLen, n, err := ReadVarIntBytes(body)
		if err != nil {
			return Packet{}, err
		}
		body = body[n:]
		if dataLen != 0 {
			if int(dataLen) > MaxPacketSize {
				return Packet{}, ErrPacketTooLarge
			}
			zr, err := zlib.NewReader(byteReader(body))
			if err != nil {
				return Packet{}, err
			}
			decoded := make([]byte, dataLen)
			if _, err := io.ReadFull(zr, decoded); err != nil {
				_ = zr.Close()
				return Packet{}, err
			}
			_ = zr.Close()
			body = decoded
		}
	}

	id, n, err := ReadVarIntBytes(body)
	if err != nil {
		return Packet{}, err
	}
	payload := body[n:]
	return Packet{ID: id, Payload: payload}, nil
}

// WritePacket 组装为单个 []byte 后一次 Write 发出。
// 无 framer 级互斥锁，多 goroutine 并发调用由底层 *net.TCPConn 的 fd.writeLock 保证不交错。
func (f *Framer) WritePacket(p Packet) error {
	out := f.encodePacket(p)
	_, err := f.w.Write(out)
	return err
}

// encodePacket 把一个 packet 序列化为完整的可发送字节序列（含 VarInt 长度头）。
// 调用者随后做一次 io.Writer.Write 即可——这是 sing-box 风格的 vectorised write 思路。
func (f *Framer) encodePacket(p Packet) []byte {
	idLen := VarIntLen(p.ID)
	bodyLen := idLen + len(p.Payload)

	if f.compThresh < 0 {
		// 未启用压缩：[VarInt(bodyLen)][VarInt(packetID)][payload]
		out := make([]byte, 0, MaxVarIntBytes+bodyLen)
		out = AppendVarInt(out, int32(bodyLen))
		out = AppendVarInt(out, p.ID)
		out = append(out, p.Payload...)
		return out
	}

	if bodyLen < f.compThresh {
		// 启用压缩但本包不压缩：dataLen=0
		// [VarInt(1+bodyLen)][VarInt(0)][VarInt(packetID)][payload]
		out := make([]byte, 0, MaxVarIntBytes+1+bodyLen)
		out = AppendVarInt(out, int32(1+bodyLen))
		out = AppendVarInt(out, 0)
		out = AppendVarInt(out, p.ID)
		out = append(out, p.Payload...)
		return out
	}

	// 压缩路径
	rawBody := make([]byte, 0, bodyLen)
	rawBody = AppendVarInt(rawBody, p.ID)
	rawBody = append(rawBody, p.Payload...)

	compressed, err := zlibCompress(rawBody)
	if err != nil {
		// 压缩失败极少见；回退到不压缩格式
		out := make([]byte, 0, MaxVarIntBytes+1+bodyLen)
		out = AppendVarInt(out, int32(1+bodyLen))
		out = AppendVarInt(out, 0)
		out = append(out, rawBody...)
		return out
	}

	dataLen := int32(bodyLen)
	headerLen := VarIntLen(dataLen) + len(compressed)
	out := make([]byte, 0, MaxVarIntBytes+headerLen)
	out = AppendVarInt(out, int32(headerLen))
	out = AppendVarInt(out, dataLen)
	out = append(out, compressed...)
	return out
}

// SwapStreams 在加密协商后重新包裹底层 conn。
func (f *Framer) SwapStreams(r io.Reader, w io.Writer) {
	f.r = bufio.NewReader(r)
	f.w = w
}

// readVarIntFromReader 从 bufio.Reader 读 VarInt。
func readVarIntFromReader(r *bufio.Reader) (int32, int, error) {
	var (
		value uint32
		pos   uint
		n     int
	)
	for {
		b, err := r.ReadByte()
		if err != nil {
			return 0, n, err
		}
		n++
		value |= uint32(b&0x7F) << pos
		if b&0x80 == 0 {
			return int32(value), n, nil
		}
		pos += 7
		if pos >= 32 {
			return 0, n, ErrVarIntTooLong
		}
	}
}

type byteReader []byte

func (b byteReader) Read(p []byte) (int, error) {
	if len(b) == 0 {
		return 0, io.EOF
	}
	n := copy(p, b)
	return n, nil
}

func zlibCompress(in []byte) ([]byte, error) {
	bw := &writeBuf{}
	zw := zlib.NewWriter(bw)
	if _, err := zw.Write(in); err != nil {
		_ = zw.Close()
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return bw.b, nil
}

type writeBuf struct{ b []byte }

func (w *writeBuf) Write(p []byte) (int, error) {
	w.b = append(w.b, p...)
	return len(p), nil
}
