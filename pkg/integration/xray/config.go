// Package xray 将 MCTE 注册为 Xray 自定义 inbound/outbound。
package xray

// UserConfig 单个 MCTE 用户。
type UserConfig struct {
	Name  string `json:"name,omitempty"`
	UUID  string `json:"uuid"`
	Level int32  `json:"level,omitempty"`
}

// MCTEConfig 对应 Xray inbound settings.mcteSettings。
type MCTEConfig struct {
	Mode           string       `json:"mode,omitempty"` // forward/virtual/hybrid
	JavaEnabled    bool         `json:"javaEnabled"`
	BedrockEnabled bool         `json:"bedrockEnabled"`
	MaxSessions    int          `json:"maxSessions,omitempty"`
	Channel        string       `json:"channel,omitempty"`
	UUIDField      string       `json:"uuidField,omitempty"`
	TargetField    string       `json:"targetField,omitempty"`
	Users          []UserConfig `json:"users"`
	Fallback       []string     `json:"fallback"` // 真实 MC server 列表
	ListenTCP      string       `json:"listenTcp,omitempty"`
	ListenUDP      string       `json:"listenUdp,omitempty"`
}

// MCTEClientConfig outbound 配置（指向远端 MCTE inbound）。
type MCTEClientConfig struct {
	Server     string `json:"server"`
	ServerPort uint16 `json:"serverPort"`
	UUID       string `json:"uuid"`
	Network    string `json:"network,omitempty"` // tcp / udp / "" (=tcp)
	Channel    string `json:"channel,omitempty"`
	// Bedrock 用：clientData JWT claim 字段名（应与 inbound 一致）
	UUIDField   string `json:"uuidField,omitempty"`
	TargetField string `json:"targetField,omitempty"`
}

// TransportName Xray 中的注册名。
const TransportName = "minecraft"
