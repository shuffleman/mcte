package java

import "encoding/binary"

// KeepAlive：双向，仅一个 long id。
type KeepAlive struct {
	ID int64
}

func EncodeKeepAlive(p *KeepAlive) []byte {
	w := NewWriterCap(8)
	w.WriteInt64(p.ID)
	return w.Bytes()
}

func DecodeKeepAlive(payload []byte) (*KeepAlive, error) {
	r := NewReader(payload)
	id, err := r.ReadInt64()
	if err != nil {
		return nil, err
	}
	return &KeepAlive{ID: id}, nil
}

// SyncPlayerPosition：服务端 -> 客户端，坐标 + flags + teleport id。1.21.2+ 结构有改动；
// 这里给出 1.20.6 兼容的最小可用形式：double x,y,z + float yaw,pitch + byte flags + VarInt teleportID。
type SyncPlayerPosition struct {
	X, Y, Z    float64
	Yaw, Pitch float32
	Flags      byte
	TeleportID int32
}

func EncodeSyncPlayerPosition(p *SyncPlayerPosition) []byte {
	w := NewWriterCap(40)
	w.WriteFloat64(p.X)
	w.WriteFloat64(p.Y)
	w.WriteFloat64(p.Z)
	w.WriteFloat32(p.Yaw)
	w.WriteFloat32(p.Pitch)
	_ = w.WriteByte(p.Flags)
	w.WriteVarInt(p.TeleportID)
	return w.Bytes()
}

// GameEvent：event byte + value float32。world loaded 通常 event=13 value=0。
type GameEvent struct {
	Event byte
	Value float32
}

func EncodeGameEvent(p *GameEvent) []byte {
	w := NewWriterCap(8)
	_ = w.WriteByte(p.Event)
	w.WriteFloat32(p.Value)
	return w.Bytes()
}

// ChunkDataAndUpdateLight：发送一个 (chunkX, chunkZ) 的全空气 chunk。
// 1.18 引入 Paletted Container，1.17 及之前格式完全不同；
// 这里只覆盖 1.18+ 的现代格式，符合本项目支持版本范围。
// data 字段为 NBT(Heightmaps, 简化为空 compound) + ChunkSection 数组 + 块实体(0) + light（4 个 BitSet 长度 0）。
type ChunkData struct {
	ChunkX int32
	ChunkZ int32
}

// EncodeChunkData 输出一个所有 section 为空气的 chunk，适用于 1.18+。
func EncodeChunkData(p *ChunkData, sectionCount int) []byte {
	w := NewWriterCap(512)
	w.WriteInt32(p.ChunkX)
	w.WriteInt32(p.ChunkZ)

	// Heightmaps：空 NBT compound（一个 TAG_End 0x00），1.20.2+ 使用网络 NBT（无名根）。
	// 兼容形态：写入单字节 0。
	_ = w.WriteByte(0x0A) // TAG_Compound
	w.WriteUint16(0)      // 空名
	_ = w.WriteByte(0x00) // TAG_End

	// Data：sections，每节包含 nonAirCount(short)=0 + 块状态调色板（单值=0=air）+ 生物群系调色板（单值=0=plains）。
	chunkSectionBytes := encodeEmptyChunkSection()
	allSections := make([]byte, 0, len(chunkSectionBytes)*sectionCount)
	for i := 0; i < sectionCount; i++ {
		allSections = append(allSections, chunkSectionBytes...)
	}
	w.WriteVarInt(int32(len(allSections)))
	w.WriteRaw(allSections)

	// block entities count = 0
	w.WriteVarInt(0)

	// light data — 简化：所有 bitset 长度均为 0，sky/block light arrays 均为 0。
	w.WriteVarInt(0) // sky light mask bitset length
	w.WriteVarInt(0) // block light mask
	w.WriteVarInt(0) // empty sky light mask
	w.WriteVarInt(0) // empty block light mask
	w.WriteVarInt(0) // sky light arrays count
	w.WriteVarInt(0) // block light arrays count
	return w.Bytes()
}

// encodeEmptyChunkSection 输出 1.18+ 全空气的单 section。
func encodeEmptyChunkSection() []byte {
	buf := make([]byte, 0, 32)
	// non-air count (short, big endian) = 0
	buf = binary.BigEndian.AppendUint16(buf, 0)
	// Block states palette container: single-valued palette
	buf = append(buf, 0)  // bits per entry = 0
	buf = AppendVarInt(buf, 0) // palette = [0] (air)
	buf = AppendVarInt(buf, 0) // data array length = 0

	// Biomes palette container: single-valued palette
	buf = append(buf, 0)
	buf = AppendVarInt(buf, 0) // palette = [0] (plains biome)
	buf = AppendVarInt(buf, 0) // data array length = 0
	return buf
}

// PlayLogin 发送一个最小化的 Play Login 包。
// 1.20.5+ 结构较复杂；为隧道虚拟会话准备的字段表如下：
//   int entityID; bool isHardcore; VarInt dimensionCount; for(dim): String;
//   VarInt maxPlayers; VarInt viewDistance; VarInt simulationDistance;
//   bool reducedDebugInfo; bool enableRespawnScreen; bool doLimitedCrafting;
//   VarInt dimensionType; String dimensionName; long hashedSeed;
//   byte gamemode; byte previousGamemode; bool isDebug; bool isFlat;
//   bool hasDeathLocation; (optional String,Position); VarInt portalCooldown;
//   bool enforcesSecureChat;
// 隧道客户端配合服务器实现即可，这里给出 1.20.6+ 通用字段。
type PlayLogin struct {
	EntityID            int32
	IsHardcore          bool
	Dimensions          []string
	MaxPlayers          int32
	ViewDistance        int32
	SimulationDistance  int32
	ReducedDebugInfo    bool
	EnableRespawnScreen bool
	DoLimitedCrafting   bool
	DimensionType       int32 // registry id
	DimensionName       string
	HashedSeed          int64
	Gamemode            byte // 3 = Spectator
	PreviousGamemode    byte // 0xFF means -1
	IsDebug             bool
	IsFlat              bool
	HasDeathLocation    bool
	PortalCooldown      int32
	EnforcesSecureChat  bool
}

func EncodePlayLogin(p *PlayLogin) []byte {
	w := NewWriterCap(128)
	w.WriteInt32(p.EntityID)
	w.WriteBool(p.IsHardcore)
	w.WriteVarInt(int32(len(p.Dimensions)))
	for _, d := range p.Dimensions {
		w.WriteString(d)
	}
	w.WriteVarInt(p.MaxPlayers)
	w.WriteVarInt(p.ViewDistance)
	w.WriteVarInt(p.SimulationDistance)
	w.WriteBool(p.ReducedDebugInfo)
	w.WriteBool(p.EnableRespawnScreen)
	w.WriteBool(p.DoLimitedCrafting)
	w.WriteVarInt(p.DimensionType)
	w.WriteString(p.DimensionName)
	w.WriteInt64(p.HashedSeed)
	_ = w.WriteByte(p.Gamemode)
	_ = w.WriteByte(p.PreviousGamemode)
	w.WriteBool(p.IsDebug)
	w.WriteBool(p.IsFlat)
	w.WriteBool(p.HasDeathLocation)
	w.WriteVarInt(p.PortalCooldown)
	w.WriteBool(p.EnforcesSecureChat)
	return w.Bytes()
}
