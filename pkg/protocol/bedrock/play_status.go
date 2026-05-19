package bedrock

import "encoding/binary"

// EncodePlayStatusPayload PlayStatus 包 payload：4B 大端 int32 状态码。
func EncodePlayStatusPayload(status int32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(status))
	return buf
}

// EncodeDisconnectPayload Disconnect 包 payload。
// 字段：bool hideDisconnectScreen + (可选) String reason。
func EncodeDisconnectPayload(reason string, hide bool) []byte {
	buf := make([]byte, 0, 1+1+len(reason))
	if hide {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
		buf = AppendString(buf, reason)
	}
	return buf
}

// EncodeResourcePacksInfo 最小化 ResourcePacksInfo（无任何包）。
// 字段：bool mustAccept + bool hasAddon + bool hasScripts + VarUint behavior count(0) + VarUint resource count(0)。
func EncodeResourcePacksInfoPayload() []byte {
	buf := make([]byte, 0, 8)
	buf = append(buf, 0) // mustAccept
	buf = append(buf, 0) // hasAddon
	buf = append(buf, 0) // hasScripts
	buf = AppendVarUint32(buf, 0) // behavior count
	buf = AppendVarUint32(buf, 0) // resource count
	return buf
}

// EncodeResourcePackStackPayload 空堆叠。
// 字段：bool mustAccept + VarUint behavior(0) + VarUint resource(0) + String gameVersion + Int32 experiments(0) + bool experimentsPrevToggled。
func EncodeResourcePackStackPayload(gameVersion string) []byte {
	buf := make([]byte, 0, 16+len(gameVersion))
	buf = append(buf, 0)
	buf = AppendVarUint32(buf, 0)
	buf = AppendVarUint32(buf, 0)
	buf = AppendString(buf, gameVersion)
	buf = binary.LittleEndian.AppendUint32(buf, 0)
	buf = append(buf, 0)
	return buf
}
