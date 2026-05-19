// Package tunnel 实现隧道客户端的握手、虚拟会话与桥接。
package tunnel

import (
	"encoding/binary"
	"errors"

	"github.com/shuffleman/mcte/pkg/protocol/java"
)

// TunnelMeta 隧道协商出的目标信息。
type TunnelMeta struct {
	Version uint8
	Host    string
	Port    uint16
}

// NegotiateChannel 默认协商 channel 名。
const NegotiateChannel = "mcte:negotiate"

// encodeNegotiateData 编码协商请求 data（这里发空 0 字节即可，data 仅响应用）。
func encodeNegotiateData() []byte { return nil }

// DecodeNegotiateResponse 解析客户端在 Login Plugin Response.data 中提交的元数据。
// 格式：1B version + 2B(BE) hostLen + host + 2B(BE) port。
func DecodeNegotiateResponse(data []byte) (*TunnelMeta, error) {
	if len(data) < 1+2 {
		return nil, errors.New("tunnel: negotiate response too short")
	}
	ver := data[0]
	off := 1
	hostLen := binary.BigEndian.Uint16(data[off : off+2])
	off += 2
	if off+int(hostLen)+2 > len(data) {
		return nil, errors.New("tunnel: negotiate response truncated")
	}
	host := string(data[off : off+int(hostLen)])
	off += int(hostLen)
	port := binary.BigEndian.Uint16(data[off : off+2])
	return &TunnelMeta{Version: ver, Host: host, Port: port}, nil
}

// BuildLoginPluginRequest 构造发给客户端的协商 LoginPluginRequest 包。
func BuildLoginPluginRequest(msgID int32, channel string) *java.LoginPluginRequest {
	if channel == "" {
		channel = NegotiateChannel
	}
	return &java.LoginPluginRequest{
		MessageID: msgID,
		Channel:   channel,
		Data:      encodeNegotiateData(),
	}
}
