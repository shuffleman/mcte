// Package singbox 把 MCTE 注册为 sing-box 的 inbound 类型 "minecraft"。
package singbox

// User 单个 MCTE 用户。
type User struct {
	Name  string `json:"name,omitempty"`
	UUID  string `json:"uuid"`
	Level int32  `json:"level,omitempty"`
}

// Options sing-box JSON 配置中的 minecraft inbound 选项。
type Options struct {
	Listen         string   `json:"listen,omitempty"`
	ListenUDP      string   `json:"listen_udp,omitempty"`
	Channel        string   `json:"channel,omitempty"`
	UUIDField      string   `json:"uuid_field,omitempty"`
	TargetField    string   `json:"target_field,omitempty"`
	Fallback       []string `json:"fallback"`
	Users          []User   `json:"users"`
	MaxSessions    int      `json:"max_sessions,omitempty"`
	JavaEnabled    bool     `json:"java_enabled,omitempty"`
	BedrockEnabled bool     `json:"bedrock_enabled,omitempty"`
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
