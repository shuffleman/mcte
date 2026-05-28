package client

import (
	"math/rand"
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
//
// 注意：启用熵前缀时 Payload 是**已编码的 wire 字节**（含 [1B prefixLen][prefix][真实数据]），
// 不是裸用户数据。调用方据此整体写出 PluginMessage；服务端按同一格式剥离。
type MimicChunk struct {
	Channel string
	Payload []byte
	Action  MimicAction
}

// MimicProfile 把上行（C→S）流量整形成"真实 MC 客户端"的包大小/时序分布。
//
// 设计依据（compare_report.md）：真实 MC 的 C→S 几乎全是 ~44 字节小包
// （movement / keepalive），下行（S→C）才是大 chunk。MCTE 此前把 C→S 也做成
// 大包（承载 HTTP 上行），导致 c2s_size_mean/max、协议指纹、burst/IAT 全部暴露。
//
// 本 profile 让 C→S：
//   - 拆成 C2SMinFrame..C2SMaxFrame 字节的小帧（默认 16..64），funnel 到 move channel；
//   - idle 时按 MoveTickInterval（默认 50ms / 20Hz）持续发 MoveSizeMin..Max 字节的
//     低熵"移动包"到 idle channel（服务端丢弃），复现 MC 连续密集小包流；
//   - 可选熵前缀：每帧前置 [1B prefixLen][prefixLen 个 0x00-heavy 字节]，抬升
//     pl_hfb_c2s_top5_ratio（载荷高频字节占比）逼近 MC 明文。
//
// 协议保证：所有 channel 以 ChannelPrefix 开头；inbound 用前缀匹配统一识别。
type MimicProfile struct {
	// ChannelPrefix 所有伪装 channel 共享的前缀（inbound 用此识别隧道流量）。默认 "mcte:"。
	ChannelPrefix string

	// channel 后缀。完整 channel 名 = ChannelPrefix + 后缀。
	MoveSuffix  string
	FlySuffix   string
	ChunkSuffix string
	IdleSuffix  string

	// 旧阈值字段（保留兼容，当前 C→S 路径不再使用，仅 String/调试参考）。
	MoveThreshold int
	FlyThreshold  int
	ChunkSize     int

	// C2SMinFrame/C2SMaxFrame：C→S 真实数据每帧字节范围（随机化避免固定大小成新指纹）。
	// 默认 16..64，使 wire 包基本 < 100 字节，匹配 MC "C→S 95%+ < 100B"。
	C2SMinFrame int
	C2SMaxFrame int

	// EntropyPrefixMin/Max：每个 C→S 数据帧前置的低熵字节数范围（0 = 关闭）。
	// 默认关闭；平衡档建议 8..16。需服务端 MimicMatcher.FramePrefix 配套剥离。
	EntropyPrefixMin int
	EntropyPrefixMax int

	// HeartbeatInterval 兼容旧字段；若 MoveTickInterval 为 0 则回退用它。
	HeartbeatInterval time.Duration

	// MoveTickInterval：idle 时补发"移动包"的周期。默认 50ms（约 20 tick/s）。0 = 禁用。
	MoveTickInterval time.Duration
	// MoveSizeMin/Max：idle 移动包字节范围。默认 28..44（模拟 MovePlayerPos/PosRot）。
	MoveSizeMin int
	MoveSizeMax int

	// C2SRateBytesPerSec：C→S 发送速率上限（字节/秒，0 = 不限速）。
	// 关键：开启后逐帧 pacing 让小帧之间有时间间隔，NODELAY 的 TCP 才会把它们
	// 当独立小包发出（否则连续 flush 会被内核 coalesce 成 MSS 大 segment，小帧白做）。
	// 代价是限 C→S 上行速率；上行通常是 HTTP 请求（小），影响可控。
	C2SRateBytesPerSec int
}

// DefaultProfile 推荐配置（平衡档：小帧 + 移动流，熵前缀默认关闭）。
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
		C2SMinFrame:       16,
		C2SMaxFrame:       64,
		EntropyPrefixMin:  0,
		EntropyPrefixMax:  0,
		HeartbeatInterval: 1500 * time.Millisecond,
		MoveTickInterval:  50 * time.Millisecond,
		MoveSizeMin:       28,
		MoveSizeMax:       44,
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
	if p.C2SMinFrame <= 0 {
		p.C2SMinFrame = 16
	}
	if p.C2SMaxFrame <= 0 {
		p.C2SMaxFrame = 64
	}
	if p.C2SMaxFrame < p.C2SMinFrame {
		p.C2SMaxFrame = p.C2SMinFrame
	}
	if p.EntropyPrefixMin < 0 {
		p.EntropyPrefixMin = 0
	}
	if p.EntropyPrefixMax < p.EntropyPrefixMin {
		p.EntropyPrefixMax = p.EntropyPrefixMin
	}
	if p.EntropyPrefixMax > 255 {
		p.EntropyPrefixMax = 255
	}
	if p.MoveTickInterval == 0 {
		p.MoveTickInterval = p.HeartbeatInterval
	}
	if p.MoveSizeMin <= 0 {
		p.MoveSizeMin = 28
	}
	if p.MoveSizeMax < p.MoveSizeMin {
		p.MoveSizeMax = p.MoveSizeMin
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

// randIntn 安全的 [0,n) 随机；n<=0 返回 0。
func randIntn(n int) int {
	if n <= 0 {
		return 0
	}
	return rand.Intn(n)
}

// encodeC2SFrame 编码一帧 C→S wire payload：[1B prefixLen][prefixLen 低熵字节][真实数据]。
// prefixLen 由 EntropyPrefixMin..Max 随机决定（0 = 仅 1 字节头，无填充）。
// 低熵填充偏向 MC 高频字节 0x00（少量 0x0F），抬升接收侧 top5 字节占比。
func (p *MimicProfile) encodeC2SFrame(data []byte) []byte {
	prefixLen := 0
	if p.EntropyPrefixMax > 0 {
		span := p.EntropyPrefixMax - p.EntropyPrefixMin
		prefixLen = p.EntropyPrefixMin + randIntn(span+1)
	}
	out := make([]byte, 0, 1+prefixLen+len(data))
	out = append(out, byte(prefixLen))
	for i := 0; i < prefixLen; i++ {
		if randIntn(8) == 0 {
			out = append(out, 0x0F)
		} else {
			out = append(out, 0x00)
		}
	}
	out = append(out, data...)
	return out
}

// Pack 把上行（C→S）data 切成若干小帧 MimicChunk，全部走 move channel。
// 每个 chunk.Payload 是已编码 wire 字节（含 1 字节帧头 + 可选熵前缀 + 数据切片）。
// 服务端按 stripFramePrefix 同格式剥离即可拼回原始字节流。
func (p *MimicProfile) Pack(data []byte) []MimicChunk {
	p.normalize()
	if len(data) == 0 {
		return nil
	}
	moveCh := p.ChannelPrefix + p.MoveSuffix
	var out []MimicChunk
	for len(data) > 0 {
		span := p.C2SMaxFrame - p.C2SMinFrame
		take := p.C2SMinFrame + randIntn(span+1)
		if take > len(data) {
			take = len(data)
		}
		frame := p.encodeC2SFrame(data[:take])
		out = append(out, MimicChunk{Channel: moveCh, Payload: frame, Action: ActionMove})
		data = data[take:]
	}
	return out
}

// MovePacket 构造一个 idle 移动包载荷：MoveSizeMin..Max 字节的低熵字节
// （模拟 MC 移动包坐标多为小值/0）。发到 idle channel，服务端整体丢弃。
func (p *MimicProfile) MovePacket() []byte {
	p.normalize()
	span := p.MoveSizeMax - p.MoveSizeMin
	n := p.MoveSizeMin + randIntn(span+1)
	buf := make([]byte, n)
	for i := range buf {
		if randIntn(6) == 0 {
			buf[i] = byte(randIntn(256))
		}
		// 否则保持 0x00（低熵）
	}
	return buf
}

// Heartbeat 构造一个 idle channel 的移动包 chunk（供 heartbeatLoop 使用）。
func (p *MimicProfile) Heartbeat() MimicChunk {
	p.normalize()
	return MimicChunk{
		Channel: p.ChannelPrefix + p.IdleSuffix,
		Payload: p.MovePacket(),
		Action:  ActionIdle,
	}
}

// IsTunnelChannel 判断 channel 是否为本 profile 的隧道 channel。
func (p *MimicProfile) IsTunnelChannel(ch string) bool {
	p.normalize()
	return strings.HasPrefix(ch, p.ChannelPrefix)
}

// IsIdleChannel 判断是否是 idle 心跳 channel（数据 payload 应当被丢弃）。
func (p *MimicProfile) IsIdleChannel(ch string) bool {
	p.normalize()
	return ch == p.ChannelPrefix+p.IdleSuffix
}
