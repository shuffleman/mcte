// Package singbox 把 MCTE 注册为 sing-box 的 inbound 类型 "minecraft"。
package singbox

// User 单个 MCTE 用户。
type User struct {
	Name  string `json:"name,omitempty"`
	UUID  string `json:"uuid"`
	Level int32  `json:"level,omitempty"`
}

// Options sing-box JSON 配置中的 minecraft inbound 选项。
//
// Fallback 取值优先级（按协议各自取）：
//   Java (TCP):    FallbackTCP → Fallback
//   Bedrock (UDP): FallbackUDP → Fallback
//
// 推荐分协议配置：
//   "fallback_tcp": ["127.0.0.1:25566"]
//   "fallback_udp": ["127.0.0.1:19133"]
//
// 旧写法仍兼容（fallback 同时给两边）。
type Options struct {
	Listen         string   `json:"listen,omitempty"`
	ListenUDP      string   `json:"listen_udp,omitempty"`
	MOTD           string   `json:"motd,omitempty"`
	Channel        string   `json:"channel,omitempty"`
	UUIDField      string   `json:"uuid_field,omitempty"`
	TargetField    string   `json:"target_field,omitempty"`
	Fallback       []string `json:"fallback,omitempty"`     // 通用 fallback（向后兼容）
	FallbackTCP    []string `json:"fallback_tcp,omitempty"` // Java 透传后端
	FallbackUDP    []string `json:"fallback_udp,omitempty"` // Bedrock 透传后端
	Users          []User   `json:"users"`
	MaxSessions    int      `json:"max_sessions,omitempty"`
	JavaEnabled    bool     `json:"java_enabled,omitempty"`
	BedrockEnabled bool     `json:"bedrock_enabled,omitempty"`
	// Mimic 启用 Java TCP 抗 DPI 流量整形识别（接受客户端 mimic 多 channel 小帧 +
	// 剥离自描述帧头 + 下行按 MC 大小切片）。需与客户端 outbound Mimic 配套。
	Mimic bool `json:"mimic,omitempty"`
	// S2CRate > 0 时限制下行（server→client）发送速率（字节/秒），压制 seq_delta_rate。
	S2CRate int `json:"s2c_rate,omitempty"`
}

// OutboundOptions outbound 配置。
type OutboundOptions struct {
	Server      string `json:"server"`
	ServerPort  uint16 `json:"server_port"`
	UUID        string `json:"uuid"`
	Network     string `json:"network,omitempty"` // tcp / udp
	Channel     string `json:"channel,omitempty"`
	UUIDField   string `json:"uuid_field,omitempty"`
	TargetField string `json:"target_field,omitempty"`
}

// TypeName sing-box 注册名。
const TypeName = "minecraft"
