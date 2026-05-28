// Package config 定义并加载 YAML 配置。
package config

import "time"

// Config 顶层配置。
type Config struct {
	Listen      ListenConfig      `yaml:"listen"`
	Tunnel      TunnelConfig      `yaml:"tunnel"`
	Users       []UserConfig      `yaml:"users"`
	Fallback    FallbackConfig    `yaml:"fallback"`
	Passthrough PassthroughConfig `yaml:"passthrough"`
	Session     SessionConfig     `yaml:"session"`
	Scheduler   SchedulerConfig   `yaml:"scheduler"`
	Log         LogConfig         `yaml:"log"`
	Metrics     MetricsConfig     `yaml:"metrics"`
	Ratelimit   RatelimitConfig   `yaml:"ratelimit"`
}

type ListenConfig struct {
	TCP string `yaml:"tcp"`
	UDP string `yaml:"udp"`
	// MOTD Bedrock 离线 ping 响应的 motd 串（完整字段格式见 raknet.BuildUnconnectedPong）。
	// 留空则使用通用默认值（不含任何品牌特征）。
	MOTD string `yaml:"motd"`
}

// TunnelConfig 隧道行为参数（与具体认证解耦）。
type TunnelConfig struct {
	Channel          string        `yaml:"channel"`
	UUIDField        string        `yaml:"uuid_field"`   // Bedrock JWT claim 字段名
	TargetField      string        `yaml:"target_field"` // Bedrock JWT claim 字段名
	WriteTimeout     time.Duration `yaml:"write_timeout"`
	KeepAliveEvery   time.Duration `yaml:"keepalive_every"`
	KeepAliveTimeout time.Duration `yaml:"keepalive_timeout"`

	// Mimic 流量画像（按数据大小选 channel 模拟玩家不同动作）。
	// Enabled=false 时使用单 Channel（兼容旧模式）。
	Mimic MimicConfig `yaml:"mimic"`

	// S2CRateBytesPerSec：服务端 → 客户端（下行）发送速率上限（字节/秒，0 = 不限速）。
	// 抗 DPI：压制监督模型头号特征 seq_delta_rate（吞吐速率）。代价是限下载速度。
	S2CRateBytesPerSec int `yaml:"s2c_rate_bytes_per_sec"`
}

// MimicConfig 流量画像（服务端 inbound 侧识别 + 下行切片）。
//
// 注意：C→S 抗 DPI 小帧整形（帧大小、移动流 tick、熵前缀）是**客户端 outbound** 行为，
// 由 integration 层 (singbox/xray) 的 Mimic / EntropyPrefix 选项控制；服务端只需用
// 同一 Prefix/IdleSuffix 识别并剥离自描述帧头（FramePrefix 恒开）。
type MimicConfig struct {
	Enabled     bool   `yaml:"enabled"`
	Prefix      string `yaml:"prefix"`       // 默认 "mcte:"
	MoveSuffix  string `yaml:"move_suffix"`  // 默认 "m"
	FlySuffix   string `yaml:"fly_suffix"`   // 默认 "f"
	ChunkSuffix string `yaml:"chunk_suffix"` // 默认 "c"
	IdleSuffix  string `yaml:"idle_suffix"`  // 默认 "i"
	MoveMax     int    `yaml:"move_max"`     // 默认 32
	FlyMax      int    `yaml:"fly_max"`      // 默认 256
	ChunkSplit  int    `yaml:"chunk_split"`  // 默认 2048
}

// UserConfig 单个用户。
type UserConfig struct {
	Name  string `yaml:"name"`
	UUID  string `yaml:"uuid"`
	Level int32  `yaml:"level"`
}

// FallbackConfig 未认证连接的去向（推荐配置真实 MC server 抗探测）。
//
// 优先级（按协议分别选取）：
//   Java (TCP):    fallback.tcp → fallback.targets → passthrough.targets
//   Bedrock (UDP): fallback.udp → fallback.targets → passthrough.targets
//
// 推荐写法：
//   fallback:
//     tcp: ["127.0.0.1:25566"]    # 真实 Java MC server
//     udp: ["127.0.0.1:19133"]    # 真实 Bedrock MC server
//
// 旧写法仍支持（targets 同时给两边）：
//   fallback:
//     targets: ["127.0.0.1:25566"]
type FallbackConfig struct {
	// Targets 通用列表；当 TCP / UDP 任一未填时作为该协议的 fallback。
	Targets     []string      `yaml:"targets"`
	TCP         []string      `yaml:"tcp"` // Java 透传后端（TCP）
	UDP         []string      `yaml:"udp"` // Bedrock 透传后端（UDP/RakNet）
	DialTimeout time.Duration `yaml:"dial_timeout"`
}

// PassthroughConfig 兼容字段：与 Fallback.Targets 等价（fallback 优先）。
type PassthroughConfig struct {
	Targets     []string      `yaml:"targets"`
	DialTimeout time.Duration `yaml:"dial_timeout"`
}

type SessionConfig struct {
	MaxConcurrent int           `yaml:"max_concurrent"`
	IdleTimeout   time.Duration `yaml:"idle_timeout"`
}

type SchedulerConfig struct {
	TickRate          int `yaml:"tick_rate"`
	SendBudgetPerTick int `yaml:"send_budget_per_tick"`
}

type LogConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

type MetricsConfig struct {
	Enabled bool   `yaml:"enabled"`
	Listen  string `yaml:"listen"`
}

// RatelimitConfig 按源 IP 限制连接速率（每秒接受多少个新连接）。
// 启用时在 accept 路径上强制执行：超出速率的新连接立即关闭。
type RatelimitConfig struct {
	Enabled    bool    `yaml:"enabled"`
	RPS        float64 `yaml:"rps"`         // 每秒允许的连接数；默认 10
	Burst      int     `yaml:"burst"`       // 突发桶大小；默认 20
	MaxEntries int     `yaml:"max_entries"` // limiters 上限；默认 16384
}

// Defaults 给出零值的合理默认。
func Defaults() Config {
	return Config{
		Listen: ListenConfig{TCP: "0.0.0.0:25565", UDP: "0.0.0.0:19132"},
		Tunnel: TunnelConfig{
			Channel:          "mcte:data",
			UUIDField:        "MCTEUuid",
			TargetField:      "MCTETarget",
			WriteTimeout:     30 * time.Second,
			KeepAliveEvery:   20 * time.Second,
			KeepAliveTimeout: 30 * time.Second,
		},
		Fallback:    FallbackConfig{DialTimeout: 5 * time.Second},
		Passthrough: PassthroughConfig{DialTimeout: 5 * time.Second},
		Session:     SessionConfig{MaxConcurrent: 10000, IdleTimeout: 5 * time.Minute},
		Scheduler:   SchedulerConfig{TickRate: 20, SendBudgetPerTick: 65536},
		Log:         LogConfig{Level: "info", Format: "console"},
		Metrics:     MetricsConfig{Enabled: false},
	}
}

// FallbackTargets 返回通用 fallback 列表（兼容旧调用方）。
// 优先 Fallback.Targets，否则 Passthrough.Targets。
func (c *Config) FallbackTargets() []string {
	if len(c.Fallback.Targets) > 0 {
		return c.Fallback.Targets
	}
	return c.Passthrough.Targets
}

// FallbackTCP 返回 Java 透传后端列表。
// 优先级：fallback.tcp → fallback.targets → passthrough.targets。
func (c *Config) FallbackTCP() []string {
	if len(c.Fallback.TCP) > 0 {
		return c.Fallback.TCP
	}
	return c.FallbackTargets()
}

// FallbackUDP 返回 Bedrock 透传后端列表。
// 优先级：fallback.udp → fallback.targets → passthrough.targets。
func (c *Config) FallbackUDP() []string {
	if len(c.Fallback.UDP) > 0 {
		return c.Fallback.UDP
	}
	return c.FallbackTargets()
}

// FallbackDialTimeout 取 fallback 或 passthrough。
func (c *Config) FallbackDialTimeout() time.Duration {
	if c.Fallback.DialTimeout > 0 {
		return c.Fallback.DialTimeout
	}
	if c.Passthrough.DialTimeout > 0 {
		return c.Passthrough.DialTimeout
	}
	return 5 * time.Second
}
