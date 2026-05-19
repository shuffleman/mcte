// Package bedrock 实现 Bedrock Edition 协议层。
//
// 本项目仅支持 1.21.4 及之后的协议版本。
// Bedrock 应用层 packet 由 RakNet 通过 Game Packet (0xFE) 携带，内部是压缩批包。
package bedrock

import (
	"encoding/binary"
	"errors"
	"io"
)

// Game Packet ID。
const IDGamePacket byte = 0xFE

// 应用层 packet ID（位于批包内单包 header 的低 10 位）。
const (
	PacketLogin                  uint32 = 0x01
	PacketPlayStatus             uint32 = 0x02
	PacketServerToClientHandshake uint32 = 0x03
	PacketDisconnect             uint32 = 0x05
	PacketResourcePacksInfo      uint32 = 0x06
	PacketResourcePackStack      uint32 = 0x07
	PacketResourcePackResponse   uint32 = 0x08
	PacketStartGame              uint32 = 0x0B
	PacketSetLocalPlayerAsInit   uint32 = 0x71

	// PacketTunnelData MCTE 自定义 packet ID，承载隧道字节。
	// 选 0x200 落在 Bedrock 未分配的高位区间，避免与现有 packet 冲突。
	PacketTunnelData uint32 = 0x200
)

// PlayStatus 值。
const (
	PlayStatusLoginSuccess     int32 = 0
	PlayStatusFailedClient     int32 = 1
	PlayStatusFailedServer     int32 = 2
	PlayStatusPlayerSpawn      int32 = 3
	PlayStatusFailedInvalidTenant int32 = 4
)

// ReadVarUint32 读 1-5 字节小端 VarInt。
func ReadVarUint32(buf []byte) (uint32, int, error) {
	var (
		v   uint32
		pos uint
	)
	for i := 0; i < len(buf) && i < 5; i++ {
		b := buf[i]
		v |= uint32(b&0x7F) << pos
		if b&0x80 == 0 {
			return v, i + 1, nil
		}
		pos += 7
	}
	if len(buf) >= 5 {
		return 0, 0, errors.New("bedrock: varuint32 too long")
	}
	return 0, 0, io.ErrUnexpectedEOF
}

// AppendVarUint32 追加 VarInt。
func AppendVarUint32(buf []byte, v uint32) []byte {
	for {
		if v&^0x7F == 0 {
			return append(buf, byte(v))
		}
		buf = append(buf, byte(v&0x7F)|0x80)
		v >>= 7
	}
}

// ReadString 读取 VarUint32 长度 + UTF-8。
func ReadString(buf []byte) (string, int, error) {
	n, m, err := ReadVarUint32(buf)
	if err != nil {
		return "", 0, err
	}
	if int(n) > len(buf)-m {
		return "", 0, io.ErrUnexpectedEOF
	}
	s := string(buf[m : m+int(n)])
	return s, m + int(n), nil
}

// AppendString 写 VarUint32 长度 + UTF-8。
func AppendString(buf []byte, s string) []byte {
	buf = AppendVarUint32(buf, uint32(len(s)))
	return append(buf, s...)
}

// PackHeader Bedrock packet header VarUint32 编码：
//   高位字段: recvSubclientID (2) | senderSubclientID (2) | packetID (10)
// 一般 subclient=0，header = packetID。
func PackHeader(packetID uint32) []byte {
	return AppendVarUint32(nil, packetID&0x3FF)
}

// EncodePacketIntoBatch 将单个 packet (header + payload) 用 VarUint32 长度前缀写入 batch buffer。
func EncodePacketIntoBatch(batch []byte, packetID uint32, payload []byte) []byte {
	header := PackHeader(packetID)
	total := len(header) + len(payload)
	batch = AppendVarUint32(batch, uint32(total))
	batch = append(batch, header...)
	batch = append(batch, payload...)
	return batch
}

// LittleInt32 工具。
func WriteInt32LE(buf []byte, v int32) []byte {
	return binary.LittleEndian.AppendUint32(buf, uint32(v))
}

// LittleVarInt zigzag-encoded var int（不少 Bedrock 字段使用）。
func WriteZigzagVarInt32(buf []byte, v int32) []byte {
	u := uint32(v<<1) ^ uint32(v>>31)
	return AppendVarUint32(buf, u)
}

func WriteZigzagVarInt64(buf []byte, v int64) []byte {
	u := uint64(v<<1) ^ uint64(v>>63)
	return AppendVarUint64(buf, u)
}

// AppendVarUint64 追加 VarLong。
func AppendVarUint64(buf []byte, v uint64) []byte {
	for {
		if v&^0x7F == 0 {
			return append(buf, byte(v))
		}
		buf = append(buf, byte(v&0x7F)|0x80)
		v >>= 7
	}
}

// ReadInt32LE 小端 int32。
func ReadInt32LE(buf []byte) (int32, int, error) {
	if len(buf) < 4 {
		return 0, 0, io.ErrUnexpectedEOF
	}
	return int32(binary.LittleEndian.Uint32(buf)), 4, nil
}

// ReadInt32BE 大端 int32（Login packet 的 protocol version 字段）。
func ReadInt32BE(buf []byte) (int32, int, error) {
	if len(buf) < 4 {
		return 0, 0, io.ErrUnexpectedEOF
	}
	return int32(binary.BigEndian.Uint32(buf)), 4, nil
}
