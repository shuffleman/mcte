// Package java 实现 Minecraft Java Edition 协议。
package java

import (
	"errors"
	"io"
)

const (
	// MaxVarIntBytes VarInt 最长 5 字节（32 位）。
	MaxVarIntBytes = 5
	// MaxVarLongBytes VarLong 最长 10 字节（64 位）。
	MaxVarLongBytes = 10
)

var (
	ErrVarIntTooLong  = errors.New("varint too long")
	ErrVarLongTooLong = errors.New("varlong too long")
)

// ReadVarInt 从 r 读取一个 VarInt，返回值与读取字节数。
func ReadVarInt(r io.ByteReader) (int32, int, error) {
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
			break
		}
		pos += 7
		if pos >= 32 {
			return 0, n, ErrVarIntTooLong
		}
	}
	return int32(value), n, nil
}

// ReadVarIntBytes 从 []byte 零拷贝读取 VarInt，返回值、消耗字节数。
// 不足以解码时返回 io.ErrUnexpectedEOF。
func ReadVarIntBytes(buf []byte) (int32, int, error) {
	var (
		value uint32
		pos   uint
	)
	for i := 0; i < len(buf) && i < MaxVarIntBytes; i++ {
		b := buf[i]
		value |= uint32(b&0x7F) << pos
		if b&0x80 == 0 {
			return int32(value), i + 1, nil
		}
		pos += 7
	}
	if len(buf) >= MaxVarIntBytes {
		return 0, 0, ErrVarIntTooLong
	}
	return 0, 0, io.ErrUnexpectedEOF
}

// WriteVarInt 将 v 编码为 VarInt 写入 w。
func WriteVarInt(w io.ByteWriter, v int32) error {
	u := uint32(v)
	for {
		if u&^0x7F == 0 {
			return w.WriteByte(byte(u))
		}
		if err := w.WriteByte(byte(u&0x7F) | 0x80); err != nil {
			return err
		}
		u >>= 7
	}
}

// AppendVarInt 将 v 追加到 buf 并返回新切片，零分配（前提是 buf 容量足够）。
func AppendVarInt(buf []byte, v int32) []byte {
	u := uint32(v)
	for {
		if u&^0x7F == 0 {
			return append(buf, byte(u))
		}
		buf = append(buf, byte(u&0x7F)|0x80)
		u >>= 7
	}
}

// VarIntLen 返回 v 编码后的字节数。
func VarIntLen(v int32) int {
	u := uint32(v)
	switch {
	case u < 1<<7:
		return 1
	case u < 1<<14:
		return 2
	case u < 1<<21:
		return 3
	case u < 1<<28:
		return 4
	default:
		return 5
	}
}

// ReadVarLong 从 r 读取 VarLong。
func ReadVarLong(r io.ByteReader) (int64, int, error) {
	var (
		value uint64
		pos   uint
		n     int
	)
	for {
		b, err := r.ReadByte()
		if err != nil {
			return 0, n, err
		}
		n++
		value |= uint64(b&0x7F) << pos
		if b&0x80 == 0 {
			break
		}
		pos += 7
		if pos >= 64 {
			return 0, n, ErrVarLongTooLong
		}
	}
	return int64(value), n, nil
}

// AppendVarLong 追加 VarLong 到 buf。
func AppendVarLong(buf []byte, v int64) []byte {
	u := uint64(v)
	for {
		if u&^0x7F == 0 {
			return append(buf, byte(u))
		}
		buf = append(buf, byte(u&0x7F)|0x80)
		u >>= 7
	}
}
