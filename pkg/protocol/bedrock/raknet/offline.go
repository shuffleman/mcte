package raknet

import (
	"encoding/binary"
	"errors"
	"net"
)

// 离线握手包 ID。
const (
	IDUnconnectedPing       byte = 0x01
	IDUnconnectedPingOpen   byte = 0x02
	IDUnconnectedPong       byte = 0x1C
	IDOpenConnectionRequest1 byte = 0x05
	IDOpenConnectionReply1   byte = 0x06
	IDOpenConnectionRequest2 byte = 0x07
	IDOpenConnectionReply2   byte = 0x08
	IDIncompatibleProtocol   byte = 0x19
)

// Magic RakNet 离线协议固定 16 字节。
var Magic = [16]byte{
	0x00, 0xFF, 0xFF, 0x00, 0xFE, 0xFE, 0xFE, 0xFE,
	0xFD, 0xFD, 0xFD, 0xFD, 0x12, 0x34, 0x56, 0x78,
}

// CurrentProtocol Bedrock 客户端常见 RakNet 协议版本（11，对应 1.16+）。
const CurrentProtocol byte = 11

// IsOfflineMessage 通过首字节快速判断是否为离线握手。
func IsOfflineMessage(b []byte) bool {
	if len(b) < 1 {
		return false
	}
	switch b[0] {
	case IDUnconnectedPing, IDUnconnectedPingOpen,
		IDOpenConnectionRequest1, IDOpenConnectionRequest2,
		IDUnconnectedPong, IDOpenConnectionReply1, IDOpenConnectionReply2:
		return true
	}
	return false
}

// UnconnectedPing 离线 ping 请求。
type UnconnectedPing struct {
	PingTime   int64
	ClientGUID int64
}

func ParseUnconnectedPing(b []byte) (*UnconnectedPing, error) {
	if len(b) < 1+8+16+8 {
		return nil, errors.New("raknet: unconnected ping too short")
	}
	if b[0] != IDUnconnectedPing && b[0] != IDUnconnectedPingOpen {
		return nil, errors.New("raknet: not a ping")
	}
	return &UnconnectedPing{
		PingTime:   int64(binary.BigEndian.Uint64(b[1:9])),
		ClientGUID: int64(binary.BigEndian.Uint64(b[25:33])),
	}, nil
}

// BuildUnconnectedPong motd 字符串遵循 Bedrock 格式：
//   MCPE;<motd>;<protocol>;<gameVersion>;<online>;<max>;<serverGUID>;<sub-motd>;<gamemode>;<gamemodeNum>;<portV4>;<portV6>;
func BuildUnconnectedPong(pingTime, serverGUID int64, motd string) []byte {
	out := make([]byte, 0, 1+8+8+16+2+len(motd))
	out = append(out, IDUnconnectedPong)
	out = binary.BigEndian.AppendUint64(out, uint64(pingTime))
	out = binary.BigEndian.AppendUint64(out, uint64(serverGUID))
	out = append(out, Magic[:]...)
	out = binary.BigEndian.AppendUint16(out, uint16(len(motd)))
	out = append(out, motd...)
	return out
}

// OpenConnectionRequest1 客户端发，包含 protocol 与 padding（用于 MTU 探测）。
type OpenConnectionRequest1 struct {
	Protocol byte
	MTU      uint16 // 实际为 padding 长度 + header 长度
}

func ParseOpenConnectionRequest1(b []byte) (*OpenConnectionRequest1, error) {
	if len(b) < 1+16+1 {
		return nil, errors.New("raknet: ocr1 too short")
	}
	if b[0] != IDOpenConnectionRequest1 {
		return nil, errors.New("raknet: not ocr1")
	}
	// 总 datagram 长度即近似 MTU；上层提供 datagramLen。
	return &OpenConnectionRequest1{
		Protocol: b[17],
	}, nil
}

// BuildOpenConnectionReply1 服务端发。
func BuildOpenConnectionReply1(serverGUID int64, mtu uint16) []byte {
	out := make([]byte, 0, 1+16+8+1+2)
	out = append(out, IDOpenConnectionReply1)
	out = append(out, Magic[:]...)
	out = binary.BigEndian.AppendUint64(out, uint64(serverGUID))
	out = append(out, 0) // use security = false
	out = binary.BigEndian.AppendUint16(out, mtu)
	return out
}

// BuildOpenConnectionRequest1 客户端发，padding 长度大致用于 MTU 探测。
func BuildOpenConnectionRequest1(mtu uint16) []byte {
	headerLen := 1 + 16 + 1
	pad := int(mtu) - headerLen - 28 // 减去 UDP/IP 头估算
	if pad < 0 {
		pad = 0
	}
	out := make([]byte, 0, headerLen+pad)
	out = append(out, IDOpenConnectionRequest1)
	out = append(out, Magic[:]...)
	out = append(out, CurrentProtocol)
	out = append(out, make([]byte, pad)...)
	return out
}

// BuildOpenConnectionRequest2 客户端发。
func BuildOpenConnectionRequest2(serverAddr *net.UDPAddr, mtu uint16, clientGUID int64) []byte {
	out := make([]byte, 0, 64)
	out = append(out, IDOpenConnectionRequest2)
	out = append(out, Magic[:]...)
	out = EncodeAddress(out, serverAddr)
	out = binary.BigEndian.AppendUint16(out, mtu)
	out = binary.BigEndian.AppendUint64(out, uint64(clientGUID))
	return out
}

// BuildIncompatibleProtocol 不兼容协议版本响应。
func BuildIncompatibleProtocol(serverGUID int64) []byte {
	out := make([]byte, 0, 1+1+16+8)
	out = append(out, IDIncompatibleProtocol)
	out = append(out, CurrentProtocol)
	out = append(out, Magic[:]...)
	out = binary.BigEndian.AppendUint64(out, uint64(serverGUID))
	return out
}

// OpenConnectionRequest2 客户端发。
type OpenConnectionRequest2 struct {
	ServerAddress *net.UDPAddr
	MTU           uint16
	ClientGUID    int64
}

func ParseOpenConnectionRequest2(b []byte) (*OpenConnectionRequest2, error) {
	if len(b) < 1+16 {
		return nil, errors.New("raknet: ocr2 too short")
	}
	if b[0] != IDOpenConnectionRequest2 {
		return nil, errors.New("raknet: not ocr2")
	}
	off := 1 + 16
	addr, n, err := DecodeAddress(b[off:])
	if err != nil {
		return nil, err
	}
	off += n
	if len(b[off:]) < 2+8 {
		return nil, errors.New("raknet: ocr2 truncated tail")
	}
	mtu := binary.BigEndian.Uint16(b[off:])
	off += 2
	guid := int64(binary.BigEndian.Uint64(b[off:]))
	return &OpenConnectionRequest2{
		ServerAddress: addr,
		MTU:           mtu,
		ClientGUID:    guid,
	}, nil
}

// BuildOpenConnectionReply2 服务端发。
func BuildOpenConnectionReply2(serverGUID int64, clientAddr *net.UDPAddr, mtu uint16) []byte {
	out := make([]byte, 0, 64)
	out = append(out, IDOpenConnectionReply2)
	out = append(out, Magic[:]...)
	out = binary.BigEndian.AppendUint64(out, uint64(serverGUID))
	out = EncodeAddress(out, clientAddr)
	out = binary.BigEndian.AppendUint16(out, mtu)
	out = append(out, 0) // use encryption = false
	return out
}

// EncodeAddress RakNet 地址编码：1B 版本 + IPv4(4B,取反) + 2B 大端端口；IPv6 形态本项目不主动使用。
func EncodeAddress(buf []byte, addr *net.UDPAddr) []byte {
	if addr == nil {
		buf = append(buf, 4)
		buf = append(buf, 0, 0, 0, 0)
		buf = binary.BigEndian.AppendUint16(buf, 0)
		return buf
	}
	ip := addr.IP.To4()
	if ip != nil {
		buf = append(buf, 4)
		buf = append(buf, ^ip[0], ^ip[1], ^ip[2], ^ip[3])
		buf = binary.BigEndian.AppendUint16(buf, uint16(addr.Port))
		return buf
	}
	// IPv6
	ip6 := addr.IP.To16()
	buf = append(buf, 6)
	buf = binary.LittleEndian.AppendUint16(buf, 23) // AF_INET6 (Linux)
	buf = binary.BigEndian.AppendUint16(buf, uint16(addr.Port))
	buf = binary.BigEndian.AppendUint32(buf, 0) // flow info
	buf = append(buf, ip6...)
	buf = binary.BigEndian.AppendUint32(buf, 0) // scope id
	return buf
}

// DecodeAddress 解 RakNet 地址。
func DecodeAddress(b []byte) (*net.UDPAddr, int, error) {
	if len(b) < 1 {
		return nil, 0, errors.New("raknet: addr ver missing")
	}
	switch b[0] {
	case 4:
		if len(b) < 7 {
			return nil, 0, errors.New("raknet: addr v4 truncated")
		}
		ip := net.IPv4(^b[1], ^b[2], ^b[3], ^b[4])
		port := binary.BigEndian.Uint16(b[5:7])
		return &net.UDPAddr{IP: ip, Port: int(port)}, 7, nil
	case 6:
		if len(b) < 29 {
			return nil, 0, errors.New("raknet: addr v6 truncated")
		}
		port := binary.BigEndian.Uint16(b[3:5])
		ip := make(net.IP, 16)
		copy(ip, b[9:25])
		return &net.UDPAddr{IP: ip, Port: int(port)}, 29, nil
	}
	return nil, 0, errors.New("raknet: addr version unknown")
}
