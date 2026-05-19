package raknet

import (
	"encoding/binary"
	"errors"
)

// Reliability 类型枚举（RakNet 标准）。
type Reliability uint8

const (
	RelUnreliable Reliability = iota
	RelUnreliableSequenced
	RelReliable
	RelReliableOrdered
	RelReliableSequenced
	RelUnreliableWithAck
	RelReliableWithAck
	RelReliableOrderedWithAck
)

// IsReliable 是否需要 ACK 跟踪。
func (r Reliability) IsReliable() bool {
	switch r {
	case RelReliable, RelReliableOrdered, RelReliableWithAck,
		RelReliableOrderedWithAck, RelReliableSequenced:
		return true
	}
	return false
}

// IsOrdered 是否携带 order index。
func (r Reliability) IsOrdered() bool {
	switch r {
	case RelReliableOrdered, RelReliableOrderedWithAck,
		RelUnreliableSequenced, RelReliableSequenced:
		return true
	}
	return false
}

// IsSequenced 是否携带 sequence index。
func (r Reliability) IsSequenced() bool {
	switch r {
	case RelUnreliableSequenced, RelReliableSequenced:
		return true
	}
	return false
}

// EncapsulatedPacket 是 RakNet 内部封装的消息。
type EncapsulatedPacket struct {
	Reliability Reliability
	MsgIndex    uint32
	OrderIndex  uint32
	OrderChan   uint8
	SeqIndex    uint32
	Fragmented  bool
	CompoundID  uint16
	FragCount   uint32
	FragIndex   uint32
	Body        []byte
}

var ErrTruncated = errors.New("raknet: truncated encapsulated packet")

// SizeOf 编码后字节数（用于 MTU 分片预算）。
func (e *EncapsulatedPacket) SizeOf() int {
	n := 3
	if e.Reliability.IsReliable() {
		n += 3
	}
	if e.Reliability.IsSequenced() {
		n += 3
	}
	if e.Reliability.IsOrdered() {
		n += 4
	}
	if e.Fragmented {
		n += 10
	}
	return n + len(e.Body)
}

// DecodeEncapsulated 从 datagram 负载中逐个解析。
func DecodeEncapsulated(buf []byte) ([]EncapsulatedPacket, error) {
	var out []EncapsulatedPacket
	for len(buf) > 0 {
		if len(buf) < 3 {
			return nil, ErrTruncated
		}
		flags := buf[0]
		reliability := Reliability((flags & 0xE0) >> 5)
		hasFrag := flags&0x10 != 0
		bitLen := binary.BigEndian.Uint16(buf[1:3])
		byteLen := int(bitLen+7) / 8
		buf = buf[3:]

		ep := EncapsulatedPacket{Reliability: reliability, Fragmented: hasFrag}

		if reliability.IsReliable() {
			if len(buf) < 3 {
				return nil, ErrTruncated
			}
			ep.MsgIndex = readUint24LE(buf[:3])
			buf = buf[3:]
		}
		if reliability.IsSequenced() {
			if len(buf) < 3 {
				return nil, ErrTruncated
			}
			ep.SeqIndex = readUint24LE(buf[:3])
			buf = buf[3:]
		}
		if reliability.IsOrdered() {
			if len(buf) < 4 {
				return nil, ErrTruncated
			}
			ep.OrderIndex = readUint24LE(buf[:3])
			ep.OrderChan = buf[3]
			buf = buf[4:]
		}
		if hasFrag {
			if len(buf) < 10 {
				return nil, ErrTruncated
			}
			ep.FragCount = binary.BigEndian.Uint32(buf[:4])
			ep.CompoundID = binary.BigEndian.Uint16(buf[4:6])
			ep.FragIndex = binary.BigEndian.Uint32(buf[6:10])
			buf = buf[10:]
		}
		if len(buf) < byteLen {
			return nil, ErrTruncated
		}
		body := make([]byte, byteLen)
		copy(body, buf[:byteLen])
		ep.Body = body
		buf = buf[byteLen:]
		out = append(out, ep)
	}
	return out, nil
}

// EncodeEncapsulated 编码一组 encapsulated packet 到 datagram payload。
func EncodeEncapsulated(packets []EncapsulatedPacket) []byte {
	total := 0
	for i := range packets {
		total += packets[i].SizeOf()
	}
	out := make([]byte, 0, total)
	for _, p := range packets {
		flags := byte(p.Reliability) << 5
		if p.Fragmented {
			flags |= 0x10
		}
		out = append(out, flags)
		out = binary.BigEndian.AppendUint16(out, uint16(len(p.Body)*8))
		if p.Reliability.IsReliable() {
			out = appendUint24LE(out, p.MsgIndex)
		}
		if p.Reliability.IsSequenced() {
			out = appendUint24LE(out, p.SeqIndex)
		}
		if p.Reliability.IsOrdered() {
			out = appendUint24LE(out, p.OrderIndex)
			out = append(out, p.OrderChan)
		}
		if p.Fragmented {
			out = binary.BigEndian.AppendUint32(out, p.FragCount)
			out = binary.BigEndian.AppendUint16(out, p.CompoundID)
			out = binary.BigEndian.AppendUint32(out, p.FragIndex)
		}
		out = append(out, p.Body...)
	}
	return out
}
