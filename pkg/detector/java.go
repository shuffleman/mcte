package detector

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"time"

	"github.com/google/uuid"

	"github.com/shuffleman/mcte/pkg/auth"
	"github.com/shuffleman/mcte/pkg/protocol/java"
)

// ClientKind 客户端类型。
type ClientKind int

const (
	KindUnknown ClientKind = iota
	KindMC                 // 真实 Minecraft 客户端，需要透传
	KindTunnel             // 隧道客户端，需要剥壳
)

// DetectResult 包含识别结果。
type DetectResult struct {
	Kind      ClientKind
	Conn      net.Conn // 必为 RestoredConn
	Handshake *java.Handshake
	Host      string // 提取出的真实 host（不含 UUID 后缀）
	User      *auth.User
}

// DetectJavaOption 选项。
type DetectJavaOption struct {
	Validator *auth.Validator
	ReadLimit int
	Deadline  time.Duration
}

// DetectJava 在 raw 上读取首个 Handshake 包，判断为 MC 或 Tunnel。
// 不消耗超过第一个完整 Handshake 包的字节，返回的 conn 必为 RestoredConn。
// 任意 IO 错误都关闭 conn 并返回 error。
//
// 抗探测：未知 UUID / 缺失 UUID 都返回 KindMC，不报错，由 fallback 透传。
func DetectJava(raw net.Conn, opt DetectJavaOption) (*DetectResult, error) {
	if opt.Deadline > 0 {
		_ = raw.SetReadDeadline(time.Now().Add(opt.Deadline))
	}
	defer raw.SetReadDeadline(time.Time{})

	var consumed bytes.Buffer
	tee := io.TeeReader(raw, &consumed)
	br := bufio.NewReader(tee)

	pktLen, _, err := readVarInt(br)
	if err != nil {
		_ = raw.Close()
		return nil, err
	}
	// detector 上限必须与 framer MaxPacketSize 对齐：
	// 早期版本卡在 8KB 与 framer 的 2MB 不一致，攻击者可发 9KB-2MB 的包做指纹识别
	// （MCTE 立即关 vs 真实 MC server 接受）。现在两者一致，行为不可区分。
	if pktLen <= 0 || int(pktLen) > java.MaxPacketSize {
		_ = raw.Close()
		return nil, java.ErrPacketTooLarge
	}
	body := make([]byte, pktLen)
	if _, err := io.ReadFull(br, body); err != nil {
		_ = raw.Close()
		return nil, err
	}

	id, n, err := java.ReadVarIntBytes(body)
	if err != nil {
		_ = raw.Close()
		return nil, err
	}
	if id != 0x00 {
		// 非 Handshake；按 MC 客户端处理
		return &DetectResult{
			Kind: KindMC,
			Conn: NewRestoredConn(consumed.Bytes(), raw),
		}, nil
	}
	hs, err := java.DecodeHandshake(body[n:])
	if err != nil {
		_ = raw.Close()
		return nil, err
	}

	host, candidate := ExtractUUIDFromAddr(hs.ServerAddress)
	res := &DetectResult{
		Conn:      NewRestoredConn(consumed.Bytes(), raw),
		Handshake: hs,
		Host:      host,
	}
	if candidate == uuid.Nil {
		res.Kind = KindMC
		return res, nil
	}
	if opt.Validator == nil {
		res.Kind = KindMC
		return res, nil
	}
	user := opt.Validator.Lookup(candidate)
	if user == nil {
		// UUID 不在用户列表 → 降级为 MC（抗探测）
		res.Kind = KindMC
		return res, nil
	}
	res.Kind = KindTunnel
	res.User = user
	return res, nil
}

// readVarInt 从 bufio.Reader 读 VarInt。
func readVarInt(br *bufio.Reader) (int32, int, error) {
	var (
		v   uint32
		pos uint
		n   int
	)
	for {
		b, err := br.ReadByte()
		if err != nil {
			return 0, n, err
		}
		n++
		v |= uint32(b&0x7F) << pos
		if b&0x80 == 0 {
			return int32(v), n, nil
		}
		pos += 7
		if pos >= 32 {
			return 0, n, java.ErrVarIntTooLong
		}
	}
}
