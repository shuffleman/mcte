package client

import (
	"strings"
	"time"
)

// MimicAction 当前数据片段映射到的"伪装玩家动作"。
type MimicAction int

const (
	// ActionIdle 空数据；模拟玩家挂机时的 ClientboundTickEnd / ServerboundPlayerInput
	ActionIdle MimicAction = iota
	// ActionMove ≤ MoveThreshold；模拟 ServerboundMovePlayerPos (~28B)
	ActionMove
	// ActionFly MoveThreshold..FlyThreshold；模拟 ServerboundMovePlayerPosRot (~36B)
	ActionFly
	// ActionChunk > FlyThreshold；模拟 ClientboundLevelChunkWithLight（区块加载，几 KB）
	ActionChunk
)

// String 调试用。
func (a MimicAction) String() string {
	switch a {
	case ActionIdle:
		return "idle"
	case ActionMove:
		return "move"
	case ActionFly:
		return "fly"
	case ActionChunk:
		return "chunk"
	}
	return "unknown"
}

// MimicChunk 一次实际写出的 PluginMessage 内容：channel + payload。
type MimicChunk struct {
	Channel string
	Payload []byte
	Action  MimicAction
}

// MimicProfile 按数据大小选择 channel + 切片策略。
//
// 设计目标：让出站隧道流量的"包大小分布"看起来像真实玩家的混合行为
// （持续小包 = 移动/输入；偶发中包 = 飞行/聊天；偶发大包 = 区块加载）。
//
// 协议保证：所有 channel 必须以 ChannelPrefix 开头；inbound 用前缀匹配统一识别。
type MimicProfile struct {
	// ChannelPrefix 所有伪装 channel 共享的前缀（inbound 用此识别隧道流量）。
	// 默认 "mcte:"。需要与 inbound 配置一致。
	ChannelPrefix string

	// MoveChannel/FlyChannel/ChunkChannel 不同动作对应的 channel 后缀。
	// 完整 channel 名 = ChannelPrefix + 后缀。
	MoveSuffix  string
	FlySuffix   string
	ChunkSuffix string
	IdleSuffix  string

	// 阈值（字节）。
	MoveThreshold int // 默认 32
	FlyThreshold  int // 默认 256

	// ChunkSize 大数据分片大小，单帧上限 32767（MC PluginMessage data 字段上限）。
	ChunkSize int // 默认 2048

	// HeartbeatInterval 上行 idle 心跳周期；0 禁用心跳。
	HeartbeatInterval time.Duration // 默认 1.5s（约 30 tick）
}

// DefaultProfile 推荐配置。
func DefaultProfile() *MimicProfile {
	return &MimicProfile{
		ChannelPrefix:     "mcte:",
		MoveSuffix:        "m",
		FlySuffix:         "f",
		ChunkSuffix:       "c",
		IdleSuffix:        "i",
		MoveThreshold:     32,
		FlyThreshold:      256,
		ChunkSize:         2048,
		HeartbeatInterval: 1500 * time.Millisecond,
	}
}

// normalize 填默认值。
func (p *MimicProfile) normalize() {
	if p.ChannelPrefix == "" {
		p.ChannelPrefix = "mcte:"
	}
	if p.MoveSuffix == "" {
		p.MoveSuffix = "m"
	}
	if p.FlySuffix == "" {
		p.FlySuffix = "f"
	}
	if p.ChunkSuffix == "" {
		p.ChunkSuffix = "c"
	}
	if p.IdleSuffix == "" {
		p.IdleSuffix = "i"
	}
	if p.MoveThreshold <= 0 {
		p.MoveThreshold = 32
	}
	if p.FlyThreshold <= 0 {
		p.FlyThreshold = 256
	}
	if p.ChunkSize <= 0 {
		p.ChunkSize = 2048
	}
	if p.ChunkSize > 32767 {
		p.ChunkSize = 32767
	}
}

// Channel 返回给定动作的完整 channel 名。
func (p *MimicProfile) Channel(a MimicAction) string {
	p.normalize()
	switch a {
	case ActionMove:
		return p.ChannelPrefix + p.MoveSuffix
	case ActionFly:
		return p.ChannelPrefix + p.FlySuffix
	case ActionChunk:
		return p.ChannelPrefix + p.ChunkSuffix
	case ActionIdle:
		return p.ChannelPrefix + p.IdleSuffix
	}
	return p.ChannelPrefix + p.MoveSuffix
}

// Pack 把上行 data 按大小分组为若干 MimicChunk。
// 单个 chunk payload 不超过 ChunkSize；同一次 Pack 内可能产生不同 channel 的 chunk
// 以提高大小分布多样性（如 1KB 数据可拆为 256B Fly + 256B Fly + 528B Chunk）。
func (p *MimicProfile) Pack(data []byte) []MimicChunk {
	p.normalize()
	if len(data) == 0 {
		return nil
	}
	var out []MimicChunk
	for len(data) > 0 {
		size := len(data)
		var (
			take    int
			action  MimicAction
			channel string
		)
		switch {
		case size <= p.MoveThreshold:
			take = size
			action = ActionMove
			channel = p.ChannelPrefix + p.MoveSuffix
		case size <= p.FlyThreshold:
			take = size
			action = ActionFly
			channel = p.ChannelPrefix + p.FlySuffix
		default:
			take = p.ChunkSize
			if take > size {
				take = size
			}
			action = ActionChunk
			channel = p.ChannelPrefix + p.ChunkSuffix
		}
		// 拷贝防止外部修改原 slice 影响入队
		buf := make([]byte, take)
		copy(buf, data[:take])
		out = append(out, MimicChunk{Channel: channel, Payload: buf, Action: action})
		data = data[take:]
	}
	return out
}

// Heartbeat 构造一个空载荷的 idle chunk。客户端可在 idle goroutine 中调用。
func (p *MimicProfile) Heartbeat() MimicChunk {
	p.normalize()
	return MimicChunk{
		Channel: p.ChannelPrefix + p.IdleSuffix,
		Payload: nil,
		Action:  ActionIdle,
	}
}

// IsTunnelChannel 判断 channel 是否为本 profile 的隧道 channel。
// inbound 在解 PluginMessage 后用此函数过滤"是否本隧道数据"。
func (p *MimicProfile) IsTunnelChannel(ch string) bool {
	p.normalize()
	return strings.HasPrefix(ch, p.ChannelPrefix)
}

// IsIdleChannel 判断是否是 idle 心跳 channel（数据 payload 应当被丢弃）。
func (p *MimicProfile) IsIdleChannel(ch string) bool {
	p.normalize()
	return ch == p.ChannelPrefix+p.IdleSuffix
}
