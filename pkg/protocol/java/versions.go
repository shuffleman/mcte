package java

// 仅支持 Minecraft Java 1.21.4 及之后的协议版本。
//
// 协议版本号参考 https://wiki.vg/Protocol_version_numbers：
//   1.21.4 -> 769
//   1.21.5 -> 770
//   1.21.6 -> 771
//   1.21.7/8 -> 772
const (
	ProtocolV1_21_4 = 769
	ProtocolV1_21_5 = 770
	ProtocolV1_21_6 = 771
	ProtocolV1_21_8 = 772
	MinSupported    = ProtocolV1_21_4
	MaxSupported    = ProtocolV1_21_8
)

// IsSupported 严格区间 [769, 772]。
func IsSupported(proto int32) bool {
	return proto >= MinSupported && proto <= MaxSupported
}

// PacketIDKey 包 ID 映射键。
type PacketIDKey struct {
	State     State
	Direction Direction
	Name      string
}

// Direction 包流向。
type Direction int8

const (
	ClientBound Direction = 0
	ServerBound Direction = 1
)

// 包名常量；所有协议依赖代码通过 PacketID(proto, state, dir, name) 查询，禁止硬编码 ID。
const (
	// handshake
	PHandshake = "handshake"

	// login
	PLoginStart          = "login_start"
	PEncryptionRequest   = "encryption_request"
	PEncryptionResponse  = "encryption_response"
	PSetCompression      = "set_compression"
	PLoginSuccess        = "login_success"
	PLoginPluginRequest  = "login_plugin_request"
	PLoginPluginResponse = "login_plugin_response"
	PLoginAcknowledged   = "login_acknowledged"

	// configuration
	PFinishConfiguration = "finish_configuration"
	PAckFinishConfig     = "ack_finish_configuration"
	PPluginMessageCB     = "configuration_plugin_message_cb"
	PPluginMessageSB     = "configuration_plugin_message_sb"
	PKnownPacks          = "configuration_known_packs"
	PAckKnownPacks       = "configuration_ack_known_packs"
	PRegistryData        = "configuration_registry_data"

	// play
	PPlayLogin           = "play_login"
	PKeepAliveCB         = "keepalive_cb"
	PKeepAliveSB         = "keepalive_sb"
	PChunkData           = "chunk_data"
	PGameEvent           = "game_event"
	PSyncPlayerPosition  = "sync_player_position"
	PPlayPluginMessageCB = "play_plugin_message_cb"
	PPlayPluginMessageSB = "play_plugin_message_sb"
)

// table121x 1.21.4 / 1.21.5 / 1.21.6 / 1.21.8 之间在我们使用的最小子集上 ID 一致；
// 若后续版本差异显现，需要按 proto 在此表追加分支。
func table121x() map[PacketIDKey]int32 {
	return map[PacketIDKey]int32{
		// handshake
		{StateHandshake, ServerBound, PHandshake}: 0x00,

		// login serverbound
		{StateLogin, ServerBound, PLoginStart}:          0x00,
		{StateLogin, ServerBound, PEncryptionResponse}:  0x01,
		{StateLogin, ServerBound, PLoginPluginResponse}: 0x02,
		{StateLogin, ServerBound, PLoginAcknowledged}:   0x03,
		// login clientbound
		{StateLogin, ClientBound, PEncryptionRequest}:  0x01,
		{StateLogin, ClientBound, PLoginSuccess}:       0x02,
		{StateLogin, ClientBound, PSetCompression}:     0x03,
		{StateLogin, ClientBound, PLoginPluginRequest}: 0x04,

		// configuration
		{StateConfiguration, ServerBound, PAckFinishConfig}: 0x03,
		{StateConfiguration, ServerBound, PPluginMessageSB}: 0x02,
		{StateConfiguration, ServerBound, PAckKnownPacks}:   0x07,
		{StateConfiguration, ClientBound, PFinishConfiguration}: 0x03,
		{StateConfiguration, ClientBound, PPluginMessageCB}:     0x01,
		{StateConfiguration, ClientBound, PKnownPacks}:          0x0E,
		{StateConfiguration, ClientBound, PRegistryData}:        0x07,

		// play clientbound (1.21.4)
		{StatePlay, ClientBound, PPlayLogin}:           0x2C,
		{StatePlay, ClientBound, PKeepAliveCB}:         0x27,
		{StatePlay, ClientBound, PChunkData}:           0x29,
		{StatePlay, ClientBound, PGameEvent}:           0x23,
		{StatePlay, ClientBound, PSyncPlayerPosition}:  0x42,
		{StatePlay, ClientBound, PPlayPluginMessageCB}: 0x19,

		// play serverbound
		{StatePlay, ServerBound, PKeepAliveSB}:         0x1A,
		{StatePlay, ServerBound, PPlayPluginMessageSB}: 0x14,
	}
}

var versionTable = func() map[int32]map[PacketIDKey]int32 {
	t := table121x()
	return map[int32]map[PacketIDKey]int32{
		ProtocolV1_21_4: t,
		ProtocolV1_21_5: t,
		ProtocolV1_21_6: t,
		ProtocolV1_21_8: t,
	}
}()

// PacketID 按协议版本查询某状态某方向某包名的 ID。
// 未知版本回退到 1.21.4 表（detector 阶段已经过 IsSupported 过滤）。
func PacketID(proto int32, state State, dir Direction, name string) int32 {
	tbl, ok := versionTable[proto]
	if !ok {
		tbl = versionTable[ProtocolV1_21_4]
	}
	id, ok := tbl[PacketIDKey{state, dir, name}]
	if !ok {
		return -1
	}
	return id
}
