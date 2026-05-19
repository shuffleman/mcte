package raknet

import (
	"encoding/binary"
	"errors"
	"net"
)

// 在线消息 ID（位于 reliable datagram 的 encapsulated body 首字节）。
const (
	IDConnectedPing             byte = 0x00
	IDConnectedPong             byte = 0x03
	IDConnectionRequest         byte = 0x09
	IDConnectionRequestAccepted byte = 0x10
	IDNewIncomingConnection     byte = 0x13
	IDDisconnectNotification    byte = 0x15
	IDDetectLostConnections     byte = 0x04
)

// ConnectionRequest 客户端发，建立 reliable 通道后立刻发送。
type ConnectionRequest struct {
	ClientGUID    int64
	ClientPingTime int64
	UseSecurity   bool
}

func ParseConnectionRequest(b []byte) (*ConnectionRequest, error) {
	if len(b) < 1+8+8+1 {
		return nil, errors.New("raknet: connection request short")
	}
	if b[0] != IDConnectionRequest {
		return nil, errors.New("raknet: not connection request")
	}
	return &ConnectionRequest{
		ClientGUID:     int64(binary.BigEndian.Uint64(b[1:9])),
		ClientPingTime: int64(binary.BigEndian.Uint64(b[9:17])),
		UseSecurity:    b[17] != 0,
	}, nil
}

// BuildConnectionRequestAccepted 服务端发回。
func BuildConnectionRequestAccepted(clientAddr *net.UDPAddr, clientPingTime, serverPingTime int64) []byte {
	out := make([]byte, 0, 192)
	out = append(out, IDConnectionRequestAccepted)
	out = EncodeAddress(out, clientAddr)
	out = binary.BigEndian.AppendUint16(out, 0) // system index
	zero := &net.UDPAddr{IP: net.IPv4zero, Port: 0}
	for i := 0; i < 20; i++ {
		out = EncodeAddress(out, zero)
	}
	out = binary.BigEndian.AppendUint64(out, uint64(clientPingTime))
	out = binary.BigEndian.AppendUint64(out, uint64(serverPingTime))
	return out
}

// ParseNewIncomingConnection 解析客户端回的 NIC。
type NewIncomingConnection struct {
	ServerPingTime int64
	ClientPingTime int64
}

func ParseNewIncomingConnection(b []byte) (*NewIncomingConnection, error) {
	if len(b) < 1 {
		return nil, errors.New("raknet: nic short")
	}
	if b[0] != IDNewIncomingConnection {
		return nil, errors.New("raknet: not nic")
	}
	// 简化解析：跳过地址段，只取末尾 16 字节
	if len(b) < 17 {
		return nil, errors.New("raknet: nic truncated")
	}
	tail := b[len(b)-16:]
	return &NewIncomingConnection{
		ServerPingTime: int64(binary.BigEndian.Uint64(tail[0:8])),
		ClientPingTime: int64(binary.BigEndian.Uint64(tail[8:16])),
	}, nil
}

// BuildConnectionRequest 客户端发起。
func BuildConnectionRequest(clientGUID, pingTime int64) []byte {
	out := make([]byte, 0, 1+8+8+1)
	out = append(out, IDConnectionRequest)
	out = binary.BigEndian.AppendUint64(out, uint64(clientGUID))
	out = binary.BigEndian.AppendUint64(out, uint64(pingTime))
	out = append(out, 0)
	return out
}

// BuildNewIncomingConnection 客户端收到 CRA 后回送。
func BuildNewIncomingConnection(serverAddr *net.UDPAddr, serverPingTime, clientPingTime int64) []byte {
	out := make([]byte, 0, 192)
	out = append(out, IDNewIncomingConnection)
	out = EncodeAddress(out, serverAddr)
	zero := &net.UDPAddr{IP: net.IPv4zero, Port: 0}
	for i := 0; i < 20; i++ {
		out = EncodeAddress(out, zero)
	}
	out = binary.BigEndian.AppendUint64(out, uint64(serverPingTime))
	out = binary.BigEndian.AppendUint64(out, uint64(clientPingTime))
	return out
}

// ParseConnectionRequestAccepted 解析。
type ConnectionRequestAccepted struct {
	ClientPingTime int64
	ServerPingTime int64
}

func ParseConnectionRequestAccepted(b []byte) (*ConnectionRequestAccepted, error) {
	if len(b) < 1+16 || b[0] != IDConnectionRequestAccepted {
		return nil, errors.New("raknet: bad CRA")
	}
	tail := b[len(b)-16:]
	return &ConnectionRequestAccepted{
		ClientPingTime: int64(binary.BigEndian.Uint64(tail[0:8])),
		ServerPingTime: int64(binary.BigEndian.Uint64(tail[8:16])),
	}, nil
}

// BuildConnectedPong 心跳 pong。
func BuildConnectedPong(pingTime, pongTime int64) []byte {
	out := make([]byte, 0, 1+16)
	out = append(out, IDConnectedPong)
	out = binary.BigEndian.AppendUint64(out, uint64(pingTime))
	out = binary.BigEndian.AppendUint64(out, uint64(pongTime))
	return out
}

// BuildConnectedPing 心跳 ping。
func BuildConnectedPing(pingTime int64) []byte {
	out := make([]byte, 0, 1+8)
	out = append(out, IDConnectedPing)
	out = binary.BigEndian.AppendUint64(out, uint64(pingTime))
	return out
}

// ParseConnectedPing 解析。
func ParseConnectedPing(b []byte) (int64, error) {
	if len(b) < 1+8 || b[0] != IDConnectedPing {
		return 0, errors.New("raknet: bad connected ping")
	}
	return int64(binary.BigEndian.Uint64(b[1:9])), nil
}
