package java

// Handshake 包结构。serverAddress 中可能带 "\x00token" 附加数据。
type Handshake struct {
	ProtocolVersion int32
	ServerAddress   string
	ServerPort      uint16
	NextState       int32 // 1=Status, 2=Login
}

// DecodeHandshake 从 packet payload 解码 Handshake。
func DecodeHandshake(payload []byte) (*Handshake, error) {
	r := NewReader(payload)
	pv, err := r.ReadVarInt()
	if err != nil {
		return nil, err
	}
	addr, err := r.ReadString(255)
	if err != nil {
		return nil, err
	}
	port, err := r.ReadUint16()
	if err != nil {
		return nil, err
	}
	next, err := r.ReadVarInt()
	if err != nil {
		return nil, err
	}
	return &Handshake{
		ProtocolVersion: pv,
		ServerAddress:   addr,
		ServerPort:      port,
		NextState:       next,
	}, nil
}

// EncodeHandshake 编码 Handshake 包（不含外层 packetID 与长度）。
func EncodeHandshake(h *Handshake) []byte {
	w := NewWriterCap(64)
	w.WriteVarInt(h.ProtocolVersion)
	w.WriteString(h.ServerAddress)
	w.WriteUint16(h.ServerPort)
	w.WriteVarInt(h.NextState)
	return w.Bytes()
}
