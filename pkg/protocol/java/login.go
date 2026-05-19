package java

// LoginStart：客户端发，含 username 与（1.20.2+）UUID。
type LoginStart struct {
	Username string
	UUID     [16]byte
}

func DecodeLoginStart(payload []byte) (*LoginStart, error) {
	r := NewReader(payload)
	name, err := r.ReadString(16)
	if err != nil {
		return nil, err
	}
	ls := &LoginStart{Username: name}
	if r.Remaining() >= 16 {
		u, err := r.ReadUUID()
		if err != nil {
			return nil, err
		}
		ls.UUID = u
	}
	return ls, nil
}

func EncodeLoginStart(ls *LoginStart) []byte {
	w := NewWriterCap(64)
	w.WriteString(ls.Username)
	w.WriteUUID(ls.UUID)
	return w.Bytes()
}

// EncryptionRequest：服务端发，不在隧道模式启用真实加密；保留结构以便兼容。
type EncryptionRequest struct {
	ServerID    string
	PublicKey   []byte
	VerifyToken []byte
	ShouldAuth  bool // 1.20.5+: 是否走 Mojang 验证；我们一律 false
}

// SetCompression：服务端发，threshold。
type SetCompression struct {
	Threshold int32
}

func EncodeSetCompression(p *SetCompression) []byte {
	w := NewWriterCap(8)
	w.WriteVarInt(p.Threshold)
	return w.Bytes()
}

// LoginSuccess：服务端发，UUID + Username + properties(空)。
type LoginSuccess struct {
	UUID       [16]byte
	Username   string
	Properties []LoginProperty
	StrictErr  bool
}

type LoginProperty struct {
	Name      string
	Value     string
	Signature string
}

func EncodeLoginSuccess(p *LoginSuccess) []byte {
	w := NewWriterCap(64)
	w.WriteUUID(p.UUID)
	w.WriteString(p.Username)
	w.WriteVarInt(int32(len(p.Properties)))
	for _, prop := range p.Properties {
		w.WriteString(prop.Name)
		w.WriteString(prop.Value)
		if prop.Signature != "" {
			w.WriteBool(true)
			w.WriteString(prop.Signature)
		} else {
			w.WriteBool(false)
		}
	}
	// 1.20.5+: strictErrorHandling bool —— 写入兼容值
	w.WriteBool(p.StrictErr)
	return w.Bytes()
}

// LoginPluginRequest：服务端发，messageID + channel + data。
type LoginPluginRequest struct {
	MessageID int32
	Channel   string
	Data      []byte
}

func EncodeLoginPluginRequest(p *LoginPluginRequest) []byte {
	w := NewWriterCap(64 + len(p.Data))
	w.WriteVarInt(p.MessageID)
	w.WriteString(p.Channel)
	w.WriteRaw(p.Data)
	return w.Bytes()
}

// LoginPluginResponse：客户端发，messageID + successful + data。
type LoginPluginResponse struct {
	MessageID  int32
	Successful bool
	Data       []byte
}

func DecodeLoginPluginResponse(payload []byte) (*LoginPluginResponse, error) {
	r := NewReader(payload)
	id, err := r.ReadVarInt()
	if err != nil {
		return nil, err
	}
	ok, err := r.ReadBool()
	if err != nil {
		return nil, err
	}
	out := &LoginPluginResponse{MessageID: id, Successful: ok}
	if ok {
		out.Data = make([]byte, r.Remaining())
		copy(out.Data, r.Bytes())
	}
	return out, nil
}
