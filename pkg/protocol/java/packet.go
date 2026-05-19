package java

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
)

// Packet 表示一个解码前/编码后的 MC 包：ID + payload。
// payload 不含包长度与 packetID 的 VarInt 前缀，纯负载。
type Packet struct {
	ID      int32
	Payload []byte
}

// State 是协议状态机的取值。
type State int32

const (
	StateHandshake     State = 0
	StateStatus        State = 1
	StateLogin         State = 2
	StateConfiguration State = 3
	StatePlay          State = 4
)

var (
	ErrPacketTooLarge = errors.New("packet too large")
	ErrShortBuffer    = errors.New("short buffer")
)

// Reader 在 []byte 上做 MC 类型的顺序读取。
type Reader struct {
	buf []byte
	off int
}

func NewReader(b []byte) *Reader { return &Reader{buf: b} }

func (r *Reader) Remaining() int { return len(r.buf) - r.off }
func (r *Reader) Bytes() []byte  { return r.buf[r.off:] }

func (r *Reader) ReadByte() (byte, error) {
	if r.off >= len(r.buf) {
		return 0, io.ErrUnexpectedEOF
	}
	b := r.buf[r.off]
	r.off++
	return b, nil
}

func (r *Reader) ReadBool() (bool, error) {
	b, err := r.ReadByte()
	return b != 0, err
}

func (r *Reader) ReadVarInt() (int32, error) {
	v, n, err := ReadVarIntBytes(r.buf[r.off:])
	if err != nil {
		return 0, err
	}
	r.off += n
	return v, nil
}

func (r *Reader) ReadVarLong() (int64, error) {
	v, n, err := readVarLongBytes(r.buf[r.off:])
	if err != nil {
		return 0, err
	}
	r.off += n
	return v, nil
}

func (r *Reader) ReadUint16() (uint16, error) {
	if r.off+2 > len(r.buf) {
		return 0, io.ErrUnexpectedEOF
	}
	v := binary.BigEndian.Uint16(r.buf[r.off:])
	r.off += 2
	return v, nil
}

func (r *Reader) ReadInt32() (int32, error) {
	if r.off+4 > len(r.buf) {
		return 0, io.ErrUnexpectedEOF
	}
	v := int32(binary.BigEndian.Uint32(r.buf[r.off:]))
	r.off += 4
	return v, nil
}

func (r *Reader) ReadInt64() (int64, error) {
	if r.off+8 > len(r.buf) {
		return 0, io.ErrUnexpectedEOF
	}
	v := int64(binary.BigEndian.Uint64(r.buf[r.off:]))
	r.off += 8
	return v, nil
}

func (r *Reader) ReadFloat32() (float32, error) {
	v, err := r.ReadInt32()
	if err != nil {
		return 0, err
	}
	return math.Float32frombits(uint32(v)), nil
}

func (r *Reader) ReadFloat64() (float64, error) {
	v, err := r.ReadInt64()
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(uint64(v)), nil
}

// ReadString 读取以 VarInt 长度前缀的 UTF-8 字符串，max 控制最大长度。
func (r *Reader) ReadString(max int) (string, error) {
	n, err := r.ReadVarInt()
	if err != nil {
		return "", err
	}
	if n < 0 || (max > 0 && int(n) > max*4) {
		return "", ErrPacketTooLarge
	}
	if r.off+int(n) > len(r.buf) {
		return "", io.ErrUnexpectedEOF
	}
	s := string(r.buf[r.off : r.off+int(n)])
	r.off += int(n)
	return s, nil
}

func (r *Reader) ReadByteArray(max int) ([]byte, error) {
	n, err := r.ReadVarInt()
	if err != nil {
		return nil, err
	}
	if n < 0 || (max > 0 && int(n) > max) {
		return nil, ErrPacketTooLarge
	}
	if r.off+int(n) > len(r.buf) {
		return nil, io.ErrUnexpectedEOF
	}
	out := make([]byte, n)
	copy(out, r.buf[r.off:r.off+int(n)])
	r.off += int(n)
	return out, nil
}

func (r *Reader) ReadRawBytes(n int) ([]byte, error) {
	if n < 0 {
		return nil, ErrShortBuffer
	}
	if r.off+n > len(r.buf) {
		return nil, io.ErrUnexpectedEOF
	}
	out := r.buf[r.off : r.off+n]
	r.off += n
	return out, nil
}

func (r *Reader) ReadUUID() ([16]byte, error) {
	var u [16]byte
	if r.off+16 > len(r.buf) {
		return u, io.ErrUnexpectedEOF
	}
	copy(u[:], r.buf[r.off:r.off+16])
	r.off += 16
	return u, nil
}

// Writer 在 []byte 上做顺序写。
type Writer struct {
	buf []byte
}

func NewWriter() *Writer { return &Writer{} }

func NewWriterCap(c int) *Writer { return &Writer{buf: make([]byte, 0, c)} }

func (w *Writer) Bytes() []byte { return w.buf }
func (w *Writer) Len() int      { return len(w.buf) }

func (w *Writer) WriteByte(b byte) error {
	w.buf = append(w.buf, b)
	return nil
}

func (w *Writer) WriteBool(v bool) {
	if v {
		w.buf = append(w.buf, 1)
	} else {
		w.buf = append(w.buf, 0)
	}
}

func (w *Writer) WriteVarInt(v int32) {
	w.buf = AppendVarInt(w.buf, v)
}

func (w *Writer) WriteVarLong(v int64) {
	w.buf = AppendVarLong(w.buf, v)
}

func (w *Writer) WriteUint16(v uint16) {
	w.buf = binary.BigEndian.AppendUint16(w.buf, v)
}

func (w *Writer) WriteInt32(v int32) {
	w.buf = binary.BigEndian.AppendUint32(w.buf, uint32(v))
}

func (w *Writer) WriteInt64(v int64) {
	w.buf = binary.BigEndian.AppendUint64(w.buf, uint64(v))
}

func (w *Writer) WriteFloat32(v float32) {
	w.WriteInt32(int32(math.Float32bits(v)))
}

func (w *Writer) WriteFloat64(v float64) {
	w.WriteInt64(int64(math.Float64bits(v)))
}

func (w *Writer) WriteString(s string) {
	w.WriteVarInt(int32(len(s)))
	w.buf = append(w.buf, s...)
}

func (w *Writer) WriteByteArray(b []byte) {
	w.WriteVarInt(int32(len(b)))
	w.buf = append(w.buf, b...)
}

func (w *Writer) WriteRaw(b []byte) {
	w.buf = append(w.buf, b...)
}

func (w *Writer) WriteUUID(u [16]byte) {
	w.buf = append(w.buf, u[:]...)
}

// readVarLongBytes 在 []byte 上零拷贝读取 VarLong。
func readVarLongBytes(buf []byte) (int64, int, error) {
	var (
		value uint64
		pos   uint
	)
	for i := 0; i < len(buf) && i < MaxVarLongBytes; i++ {
		b := buf[i]
		value |= uint64(b&0x7F) << pos
		if b&0x80 == 0 {
			return int64(value), i + 1, nil
		}
		pos += 7
	}
	if len(buf) >= MaxVarLongBytes {
		return 0, 0, ErrVarLongTooLong
	}
	return 0, 0, io.ErrUnexpectedEOF
}
