// Package raknet 实现 Bedrock 所依赖的 RakNet 可靠性层。
package raknet

import (
	"encoding/binary"
	"errors"
	"sort"
)

// Datagram 头部位标识。
const (
	FlagValid    byte = 0x80
	FlagACK      byte = 0x40
	FlagNAK      byte = 0x20
	FlagPair     byte = 0x10
	FlagContinue byte = 0x08
	FlagNeeds    byte = 0x04
)

// AckRecord 表示 ACK/NAK 中的一段连续序号范围。
type AckRecord struct {
	Start uint32
	End   uint32 // 包含
}

// EncodeAck 将记录编码为 ACK/NAK payload（不含首字节 flag）。
func EncodeAck(records []AckRecord) []byte {
	if len(records) == 0 {
		return []byte{0, 0}
	}
	out := make([]byte, 0, 2+len(records)*7)
	out = binary.BigEndian.AppendUint16(out, uint16(len(records)))
	for _, r := range records {
		if r.Start == r.End {
			out = append(out, 1)
			out = appendUint24LE(out, r.Start)
		} else {
			out = append(out, 0)
			out = appendUint24LE(out, r.Start)
			out = appendUint24LE(out, r.End)
		}
	}
	return out
}

// DecodeAck 解码 ACK/NAK payload，返回序号范围列表。
func DecodeAck(payload []byte) ([]AckRecord, error) {
	if len(payload) < 2 {
		return nil, errors.New("ack: short payload")
	}
	count := binary.BigEndian.Uint16(payload[:2])
	payload = payload[2:]
	out := make([]AckRecord, 0, count)
	for i := 0; i < int(count); i++ {
		if len(payload) < 1 {
			return nil, errors.New("ack: truncated record header")
		}
		single := payload[0] == 1
		payload = payload[1:]
		if single {
			if len(payload) < 3 {
				return nil, errors.New("ack: truncated single record")
			}
			v := readUint24LE(payload[:3])
			payload = payload[3:]
			out = append(out, AckRecord{Start: v, End: v})
		} else {
			if len(payload) < 6 {
				return nil, errors.New("ack: truncated range record")
			}
			s := readUint24LE(payload[:3])
			e := readUint24LE(payload[3:6])
			payload = payload[6:]
			out = append(out, AckRecord{Start: s, End: e})
		}
	}
	return out, nil
}

// CompactAckRanges 将无序序号列表压缩为连续范围。
func CompactAckRanges(seqs []uint32) []AckRecord {
	if len(seqs) == 0 {
		return nil
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	out := []AckRecord{{Start: seqs[0], End: seqs[0]}}
	for i := 1; i < len(seqs); i++ {
		v := seqs[i]
		last := &out[len(out)-1]
		if v == last.End+1 {
			last.End = v
		} else if v != last.End {
			out = append(out, AckRecord{Start: v, End: v})
		}
	}
	return out
}

func appendUint24LE(b []byte, v uint32) []byte {
	return append(b, byte(v), byte(v>>8), byte(v>>16))
}

func readUint24LE(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16
}
