package java

// PluginMessage 双向通用结构：channel + data（data 不含长度，read 时按剩余字节）。
type PluginMessage struct {
	Channel string
	Data    []byte
}

func EncodePluginMessage(p *PluginMessage) []byte {
	w := NewWriterCap(len(p.Channel) + len(p.Data) + 8)
	w.WriteString(p.Channel)
	w.WriteRaw(p.Data)
	return w.Bytes()
}

func DecodePluginMessage(payload []byte) (*PluginMessage, error) {
	r := NewReader(payload)
	ch, err := r.ReadString(256)
	if err != nil {
		return nil, err
	}
	data := make([]byte, r.Remaining())
	copy(data, r.Bytes())
	return &PluginMessage{Channel: ch, Data: data}, nil
}

// FinishConfiguration 双向无字段。
type FinishConfiguration struct{}

func EncodeFinishConfiguration() []byte { return nil }
