package detector

import (
	"net"
	"time"

	"github.com/shuffleman/mcte/pkg/auth"
)

// ProtocolType 网络层协议。
type ProtocolType int

const (
	ProtoJava ProtocolType = iota
	ProtoBedrock
)

// Options 公共配置。
type Options struct {
	Validator   *auth.Validator
	ReadLimit   int
	Deadline    time.Duration
	UUIDField   string
	TargetField string
}

// Detect Java 入口。Bedrock 端在 engine.handleUDP 直接调 DetectBedrock。
func Detect(raw net.Conn, opt Options) (*DetectResult, error) {
	return DetectJava(raw, DetectJavaOption{
		Validator: opt.Validator,
		ReadLimit: opt.ReadLimit,
		Deadline:  opt.Deadline,
	})
}
