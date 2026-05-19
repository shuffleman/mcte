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
}

type ListenConfig struct {
	TCP string `yaml:"tcp"`
	UDP string `yaml:"udp"`
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
}

// MimicConfig 流量画像。
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
type FallbackConfig struct {
	Targets     []string      `yaml:"targets"`
	DialTimeout time.Duration `yaml:"dial_timeout"`
}

// PassthroughConfig 兼容字段：Targets 与 Fallback.Targets 二选一；
// 同时填以 fallback 为准。
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

// FallbackTargets 合并 Fallback.Targets 与 Passthrough.Targets，前者优先。
func (c *Config) FallbackTargets() []string {
	if len(c.Fallback.Targets) > 0 {
		return c.Fallback.Targets
	}
	return c.Passthrough.Targets
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
