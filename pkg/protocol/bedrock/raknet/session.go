package raknet

import (
	"context"
	"encoding/binary"
	"errors"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shuffleman/mcte/internal/debug"
)

// ConnState 会话状态。
type ConnState int32

const (
	StateUnconnected ConnState = iota
	StateOpenReply1Sent
	StateOpenReply2Sent
	StateConnectionRequestRecv
	StateConnected
	StateClosed
)

// Session 表示一个 RakNet 会话；对上层提供应用层 packet 的读写。
// 既可作为服务端 Session（由 UDP listener 创建），也可作为客户端 Session（通过 Dial 拨号）。
type Session struct {
	conn       net.PacketConn
	remote     net.Addr
	serverGUID int64
	clientGUID int64
	isClient   bool
	mtu        uint16
	maxFrag    int

	state         atomic.Int32
	stopOnce      sync.Once
	stopped       chan struct{}
	handshakeOnce sync.Once
	handshakeDone chan struct{}

	// 接收侧
	recvWindow   *SeqWindow
	frag         *FragmentAssembler
	ackCollector *AckCollector
	ordering     [16]*OrderingBuffer
	recvCh       chan []byte
	inbox        chan []byte // datagram 入站队列，由 dispatcher 消费
	dropInbox    atomic.Uint64
	dropRecv     atomic.Uint64

	// 发送侧
	seqOut    atomic.Uint32
	msgIdxOut atomic.Uint32
	orderOut  atomic.Uint32
	seqIdxOut atomic.Uint32 // sequenced packet 计数器
	cidOut    atomic.Uint32
	resend       *ResendQueue
	writeMu      sync.Mutex
	lastDropSeen uint64 // 仅 doResend tick 访问，无并发

	lastRecv atomic.Int64
	closed   chan struct{}
}

// NewSession 由 listener 在收到 OCR1 时构造。
func NewSession(pc net.PacketConn, remote net.Addr, mtu uint16, serverGUID int64) *Session {
	if mtu < 576 {
		mtu = 576
	}
	s := &Session{
		conn:         pc,
		remote:       remote,
		serverGUID:   serverGUID,
		mtu:          mtu,
		maxFrag:      int(mtu) - 60, // 留出 RakNet/UDP/IP 头
		recvWindow:   NewSeqWindow(8192),
		frag:         NewFragmentAssembler(16 * 1024 * 1024),
		ackCollector: NewAckCollector(),
		// inbox / recvCh 容量调大：跨境高 RTT 场景下 cwnd 可能 burst 大量 reliable
		// datagram，原 512/256 容量会被 burst 打满 → Feed 丢包 → server 重传 → 加重拥塞。
		// 调到 4096/1024 后单个 session 在 cwnd=64 时仍有 64 倍 headroom。
		recvCh:        make(chan []byte, 1024),
		inbox:         make(chan []byte, 4096),
		resend:        NewResendQueue(200*time.Millisecond, 24),
		stopped:       make(chan struct{}),
		closed:        make(chan struct{}),
		handshakeDone: make(chan struct{}),
	}
	for i := range s.ordering {
		s.ordering[i] = NewOrderingBuffer()
	}
	s.state.Store(int32(StateUnconnected))
	s.lastRecv.Store(time.Now().UnixNano())
	return s
}

// Remote 对端地址。
func (s *Session) Remote() net.Addr { return s.remote }

// State 当前状态。
func (s *Session) State() ConnState { return ConnState(s.state.Load()) }

// MTU 协商出的 MTU。
func (s *Session) MTU() uint16 { return s.mtu }

// ServerGUID 服务端 GUID。
func (s *Session) ServerGUID() int64 { return s.serverGUID }

// Closed 关闭信号。
func (s *Session) Closed() <-chan struct{} { return s.closed }

// ReadApp 读取下一个应用层 packet（>= NumInternalMessages 的 packet ID 起始字节）。
// 关键：session 已 close 时，仍然 drain 剩余 recvCh 数据后再报 ErrClosed —
// 否则 select 在 closed 和 recvCh 同时 ready 时会随机选 closed 路径，丢弃已收到
// 但还没消费的应用层数据，导致上层 HTTP body 截断（schannel: server closed abruptly）。
func (s *Session) ReadApp(ctx context.Context) ([]byte, error) {
	// 优先取 recvCh 中已有数据（非阻塞 try）。
	select {
	case b, ok := <-s.recvCh:
		if !ok {
			return nil, net.ErrClosed
		}
		return b, nil
	default:
	}
	// recvCh 空时再监听三路：新数据 / close / ctx 取消。
	select {
	case b, ok := <-s.recvCh:
		if !ok {
			return nil, net.ErrClosed
		}
		return b, nil
	case <-s.closed:
		// close 后 recvCh 可能仍有未消费数据，再 drain 一次。
		select {
		case b, ok := <-s.recvCh:
			if !ok {
				return nil, net.ErrClosed
			}
			return b, nil
		default:
			return nil, net.ErrClosed
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// WriteApp 以 ReliableOrdered 发送应用层 packet（如 0xFE Game Packet 批包）。
// 阻塞至 cwnd 有空间或 ctx 取消。
func (s *Session) WriteApp(body []byte) error {
	return s.WriteAppCtx(context.Background(), body)
}

// WriteAppCtx 携带 ctx 的版本；用于上游慢消费时优雅取消。
func (s *Session) WriteAppCtx(ctx context.Context, body []byte) error {
	if debug.Enabled() {
		debug.Logf("WriteApp len=%d cwnd=%d inflight=%d", len(body), s.resend.Cwnd(), s.resend.Len())
	}
	if err := s.resend.WaitForRoom(ctx); err != nil {
		if debug.Enabled() {
			debug.Logf("WriteApp WaitForRoom err=%v cwnd=%d inflight=%d", err, s.resend.Cwnd(), s.resend.Len())
		}
		return err
	}
	return s.writeEncapsulated(body, RelReliableOrdered, 0)
}

// Close 关闭 session：先把 in-flight reliable EP flush（等待 ACK 或 maxTry 用尽），
// 再发 DisconnectNotification，最后停止 RunBackground。
//
// 如果不 flush 直接 close，RunBackground 退出 → ResendQueue 不再重传 → 缺片永远不到 →
// 对端 OrderingBuffer 卡住 → 上层协议读取超时切断（症状是 HTTP body 截断、TLS abrupt close）。
func (s *Session) Close() error {
	s.stopOnce.Do(func() {
		s.flushBeforeClose()
		_ = s.writeEncapsulated([]byte{IDDisconnectNotification}, RelReliableOrdered, 0)
		close(s.stopped)
		s.state.Store(int32(StateClosed))
		close(s.closed)
	})
	return nil
}

// flushBeforeClose 等待 ResendQueue 清空或最多 2 秒超时。
// 在此期间 RunBackground 仍在运行，会持续重传未 ACK 的 reliable EP。
// 跨境丢包链路上 2 秒已经覆盖 10 个 RTO 周期，足够让正常 in-flight 完成 ACK；
// 仍未到达的（路径黑洞）继续等也无意义，直接 close 让上层重连更快。
func (s *Session) flushBeforeClose() {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.resend.Len() == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	if debug.Enabled() {
		debug.Logf("flushBeforeClose timeout, %d entries still in-flight", s.resend.Len())
	}
}

// Feed 由 UDP listener 投入一个属于本会话的原始 datagram。
// 非阻塞：inbox 满时丢包并计入 dropInbox 计数，让出 listener 主循环。
//
// 关键不变量：ACK/NAK/离线握手 datagram **必须走快速通道同步处理**，
// 绝对不能被排在 inbox 后面等 dispatchLoop。
// 因为 dispatchLoop 处理应用层 reliable 包时可能阻塞在 recvCh（用户慢消费），
// 此时若 ACK 也排队等 dispatchLoop，就永远拿不到 ACK → 对端 cwnd 满 → 死锁。
// 实测："直接阻塞死了" 就是这个 bug。
func (s *Session) Feed(payload []byte) error {
	if len(payload) == 0 {
		return errors.New("raknet: empty datagram")
	}
	flag := payload[0]
	// 离线握手 / ACK / NAK 走快速通道
	if flag&FlagValid == 0 || flag&FlagACK != 0 || flag&FlagNAK != 0 {
		s.lastRecv.Store(time.Now().UnixNano())
		return s.processDatagram(payload)
	}
	// 普通 reliable datagram 走 inbox
	select {
	case s.inbox <- payload:
		return nil
	case <-s.closed:
		return net.ErrClosed
	default:
		n := s.dropInbox.Add(1)
		if debug.Enabled() {
			// 截断显示 seq 让排查 retransmit 是否被 inbox drop 吞掉
			var seq uint32
			if len(payload) >= 4 {
				seq = readUint24LE(payload[1:4])
			}
			debug.Logf("INBOX_DROP seq=%d total=%d (channel full, dispatchLoop too slow)", seq, n)
		}
		return nil
	}
}

// dispatchLoop 由 RunBackground 启动；从 inbox 取 datagram 顺序处理。
func (s *Session) dispatchLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopped:
			return
		case payload := <-s.inbox:
			_ = s.processDatagram(payload)
		}
	}
}

// processDatagram 同步处理一个 datagram（原 Feed 内容）。
func (s *Session) processDatagram(payload []byte) error {
	s.lastRecv.Store(time.Now().UnixNano())
	flag := payload[0]

	// 离线握手
	if flag&FlagValid == 0 {
		return s.handleOffline(payload)
	}

	// ACK
	if flag&FlagACK != 0 {
		records, err := DecodeAck(payload[1:])
		if err != nil {
			return err
		}
		if debug.Enabled() {
			n := 0
			for _, r := range records {
				n += int(r.End - r.Start + 1)
			}
			debug.Logf("recv ACK from=%s seqs=%d records=%v cwnd=%d inflight=%d",
				s.remote, n, records, s.resend.Cwnd(), s.resend.Len())
		}
		s.resend.AckRange(records)
		return nil
	}
	// NAK
	if flag&FlagNAK != 0 {
		records, err := DecodeAck(payload[1:])
		if err != nil {
			return err
		}
		for _, item := range s.resend.NakRange(records) {
			s.retransmit(item)
		}
		return nil
	}

	// reliable datagram
	if len(payload) < 4 {
		return errors.New("raknet: short reliable datagram")
	}
	seq := readUint24LE(payload[1:4])
	isNew := s.recvWindow.Receive(seq)
	if debug.Enabled() {
		debug.Logf("recv reliable datagram seq=%d isNew=%v from=%s", seq, isNew, s.remote)
	}
	// 无论新旧 datagram 都必须 ACK！
	// 关键不变量：对端 ACK 丢了会重传同一个 seq，如果接收端只对首次 ACK，
	// 重传永远拿不到 ACK → 对端 cwnd 满后 WaitForRoom 永久阻塞 → user write 卡死。
	if !s.ackCollector.Add(seq) {
		// 容量已满，先立即发出，再补加本次 seq 否则会 silent drop —
		// 对端永远收不到这条 ACK，最终触发 RTO + 重传/maxTry 用尽。
		s.flushAcks()
		s.ackCollector.Add(seq)
	}
	if !isNew {
		return nil
	}

	eps, err := DecodeEncapsulated(payload[4:])
	if err != nil {
		return err
	}
	for _, ep := range eps {
		s.handleEncapsulated(ep)
	}
	return nil
}

// HandshakeDone 等待 connected 状态。
func (s *Session) HandshakeDone() <-chan struct{} { return s.handshakeDone }

// handleOffline 处理离线握手；server 与 client 路径分支。
func (s *Session) handleOffline(payload []byte) error {
	if s.isClient {
		return s.handleOfflineClient(payload)
	}
	switch payload[0] {
	case IDOpenConnectionRequest1:
		if _, err := ParseOpenConnectionRequest1(payload); err != nil {
			return err
		}
		// 用 datagram 实际长度近似 MTU（含 UDP/IP 头）
		mtu := uint16(len(payload) + 28)
		if mtu > 1492 {
			mtu = 1492
		}
		s.mtu = mtu
		s.maxFrag = int(mtu) - 60
		_, err := s.conn.WriteTo(BuildOpenConnectionReply1(s.serverGUID, mtu), s.remote)
		s.state.Store(int32(StateOpenReply1Sent))
		return err
	case IDOpenConnectionRequest2:
		req, err := ParseOpenConnectionRequest2(payload)
		if err != nil {
			return err
		}
		if req.MTU > 1492 {
			req.MTU = 1492
		}
		s.mtu = req.MTU
		s.maxFrag = int(req.MTU) - 60
		ua, _ := s.remote.(*net.UDPAddr)
		_, err = s.conn.WriteTo(BuildOpenConnectionReply2(s.serverGUID, ua, req.MTU), s.remote)
		s.state.Store(int32(StateOpenReply2Sent))
		return err
	}
	return nil
}

// handleOfflineClient 客户端方向的离线握手响应。
func (s *Session) handleOfflineClient(payload []byte) error {
	switch payload[0] {
	case IDOpenConnectionReply1:
		// reply1: 1B id + 16B magic + 8B serverGUID + 1B secure + 2B mtu
		if len(payload) < 1+16+8+1+2 {
			return errors.New("raknet: reply1 short")
		}
		off := 1 + 16
		s.serverGUID = int64(binary.BigEndian.Uint64(payload[off : off+8]))
		off += 8 + 1
		mtu := binary.BigEndian.Uint16(payload[off : off+2])
		if mtu > 1492 {
			mtu = 1492
		}
		s.mtu = mtu
		s.maxFrag = int(mtu) - 60
		ua, _ := s.remote.(*net.UDPAddr)
		out := BuildOpenConnectionRequest2(ua, mtu, s.clientGUID)
		_, err := s.conn.WriteTo(out, s.remote)
		return err
	case IDOpenConnectionReply2:
		// reply2: id + magic + serverGUID + clientAddr + mtu + secure
		if len(payload) < 1+16+8 {
			return errors.New("raknet: reply2 short")
		}
		// 直接进入 reliable：发 ConnectionRequest
		cr := BuildConnectionRequest(s.clientGUID, time.Now().UnixMilli())
		return s.writeEncapsulated(cr, RelReliable, 0)
	case IDIncompatibleProtocol:
		return errors.New("raknet: incompatible protocol")
	}
	return nil
}

// handleEncapsulated 处理一条解封装的消息。
//
// 关键不变量：fragmented + ordered 的包分片完成后必须 **也走 ordering buffer**，
// 否则它会绕过 expected 检查直接 deliver，与同 channel 内其他非分片包到达顺序
// 不一致 → 应用层字节流乱序 → TLS 解密失败 / 协议 framing 错乱。
func (s *Session) handleEncapsulated(ep EncapsulatedPacket) {
	body := ep.Body
	if ep.Fragmented {
		if debug.Enabled() {
			debug.Logf("frag.Add cid=%d idx=%d/%d size=%d order=%d rel=%d",
				ep.CompoundID, ep.FragIndex, ep.FragCount, len(ep.Body), ep.OrderIndex, ep.Reliability)
		}
		b, done, err := s.frag.Add(ep.CompoundID, ep.FragIndex, ep.FragCount, ep.Body)
		if debug.Enabled() {
			groups, dropG, dropT, dropMT, dropMS := s.frag.Stats()
			debug.Logf("frag.Add result done=%v err=%v len=%d groups=%d drops=(maxG=%d,timeout=%d,maxTotal=%d,maxSize=%d)",
				done, err, len(b), groups, dropG, dropT, dropMT, dropMS)
		}
		if err != nil || !done {
			return
		}
		body = b
	} else if debug.Enabled() {
		debug.Logf("ep recv unfragmented order=%d rel=%d size=%d head=%02x",
			ep.OrderIndex, ep.Reliability, len(ep.Body), firstByte(ep.Body))
	}

	// 顺序通道：所有 ordered 包（含分片完成后的）都进 ordering buffer 严格按 OrderIndex 出
	if ep.Reliability.IsOrdered() {
		ch := ep.OrderChan
		if int(ch) < len(s.ordering) {
			// 构造已拼装完整 body 的"逻辑包"送进 ordering（保留 OrderIndex/OrderChan）
			logical := EncapsulatedPacket{
				Reliability: ep.Reliability,
				OrderIndex:  ep.OrderIndex,
				OrderChan:   ep.OrderChan,
				Body:        body,
			}
			released, overflow := s.ordering[ch].Push(logical)
			for _, queued := range released {
				s.deliver(queued.Body)
			}
			if overflow {
				// 缺片长时间没补上（对端 ResendQueue 已放弃），不能 silent skip
				// 否则上层 HTTP/TCP 拿到不完整数据。主动 close 让上层重连。
				if debug.Enabled() {
					debug.Logf("ordering buffer overflow ch=%d expected=%d order=%d — closing session",
						ch, s.ordering[ch].expected, ep.OrderIndex)
				}
				_ = s.Close()
			}
			return
		}
	}
	s.deliver(body)
}

// deliver 派发已就绪的消息：内部消息自处理，应用层入队。
func (s *Session) deliver(body []byte) {
	if len(body) == 0 {
		return
	}
	switch body[0] {
	case IDConnectionRequest:
		req, err := ParseConnectionRequest(body)
		if err != nil {
			return
		}
		s.state.Store(int32(StateConnectionRequestRecv))
		ua, _ := s.remote.(*net.UDPAddr)
		nowPing := time.Now().UnixMilli()
		resp := BuildConnectionRequestAccepted(ua, req.ClientPingTime, nowPing)
		_ = s.writeEncapsulated(resp, RelReliableOrdered, 0)
		return
	case IDConnectionRequestAccepted:
		cra, err := ParseConnectionRequestAccepted(body)
		if err != nil {
			return
		}
		// 客户端回 NIC
		ua, _ := s.remote.(*net.UDPAddr)
		nowMs := time.Now().UnixMilli()
		nic := BuildNewIncomingConnection(ua, cra.ServerPingTime, nowMs)
		_ = s.writeEncapsulated(nic, RelReliable, 0)
		s.state.Store(int32(StateConnected))
		s.handshakeOnce.Do(func() { close(s.handshakeDone) })
		return
	case IDNewIncomingConnection:
		if _, err := ParseNewIncomingConnection(body); err == nil {
			s.state.Store(int32(StateConnected))
			s.handshakeOnce.Do(func() { close(s.handshakeDone) })
		}
		return
	case IDConnectedPing:
		pt, err := ParseConnectedPing(body)
		if err != nil {
			return
		}
		pong := BuildConnectedPong(pt, time.Now().UnixMilli())
		// 心跳 pong 用 UnreliableSequenced：陈旧的 pong 会被对端丢弃，最新的总能到
		_ = s.writeEncapsulated(pong, RelUnreliableSequenced, 0)
		return
	case IDConnectedPong:
		return
	case IDDisconnectNotification:
		_ = s.Close()
		return
	case IDDetectLostConnections:
		return
	}
	// 应用层消息：阻塞直至消费者消费或 session 终止。
	// 不能丢——丢了会破坏 reliable ordered 语义（ACK 已发，远端不会重传）。
	// 真正的反压链：recvCh 满 → deliver 阻塞 → dispatchLoop 阻塞 → inbox 满 →
	// Feed 丢 datagram → 远端没收到 ACK → 远端 RTO 重传 + cwnd 减半，
	// 是 UDP 正确的拥塞反馈机制。
	cp := make([]byte, len(body))
	copy(cp, body)
	if debug.Enabled() {
		debug.Logf("deliver app body len=%d head=%02x recvCh_len=%d", len(body), body[0], len(s.recvCh))
	}
	select {
	case s.recvCh <- cp:
	case <-s.stopped:
	case <-s.closed:
	}
}

// DropStats 当前 inbox / recvCh 丢包计数（监控用）。
func (s *Session) DropStats() (inbox, recv uint64) {
	return s.dropInbox.Load(), s.dropRecv.Load()
}

// writeEncapsulated 内部统一发送出口。
func (s *Session) writeEncapsulated(body []byte, rel Reliability, orderChan uint8) error {
	if s.maxFrag <= 0 {
		s.maxFrag = 1200
	}
	if len(body) <= s.maxFrag {
		ep := EncapsulatedPacket{
			Reliability: rel,
			OrderChan:   orderChan,
			Body:        body,
		}
		// 计数器都是 atomic.Add(1)，即先加后返回。
		// 必须减 1 让第一个 index 从 0 开始 — 因为对端 OrderingBuffer.expected
		// 初值为 0，OrderIndex 若从 1 开始会永远在 pending 中无法释放，
		// 导致 ConnectionRequestAccepted 等握手包卡死。
		if rel.IsReliable() {
			ep.MsgIndex = (s.msgIdxOut.Add(1) - 1) & 0xFFFFFF
		}
		if rel.IsSequenced() {
			ep.SeqIndex = (s.seqIdxOut.Add(1) - 1) & 0xFFFFFF
		}
		if rel.IsOrdered() {
			ep.OrderIndex = (s.orderOut.Add(1) - 1) & 0xFFFFFF
		}
		return s.sendDatagram([]EncapsulatedPacket{ep})
	}
	// 分片
	cid := uint16(s.cidOut.Add(1) & 0xFFFF)
	orderIdx := (s.orderOut.Add(1) - 1) & 0xFFFFFF
	var seqIdx uint32
	if rel.IsSequenced() {
		seqIdx = (s.seqIdxOut.Add(1) - 1) & 0xFFFFFF
	}
	count := (len(body) + s.maxFrag - 1) / s.maxFrag
	// 均匀切片 — 避免最后一片远小于其他片造成的 DPI 特征
	// （例如 3894 字节切 4 片：[974,974,974,972] 而不是 [1200,1200,1200,294]）。
	chunkSize := (len(body) + count - 1) / count
	for i := 0; i < count; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if end > len(body) {
			end = len(body)
		}
		ep := EncapsulatedPacket{
			Reliability: rel,
			OrderChan:   orderChan,
			Fragmented:  true,
			CompoundID:  cid,
			FragCount:   uint32(count),
			FragIndex:   uint32(i),
			OrderIndex:  orderIdx,
			SeqIndex:    seqIdx,
			Body:        body[start:end],
		}
		if rel.IsReliable() {
			ep.MsgIndex = (s.msgIdxOut.Add(1) - 1) & 0xFFFFFF
		}
		if err := s.sendDatagram([]EncapsulatedPacket{ep}); err != nil {
			return err
		}
	}
	return nil
}

// sendDatagram 发送新 datagram；reliable EP 自动加入 resend 队列跟踪。
func (s *Session) sendDatagram(eps []EncapsulatedPacket) error {
	_, err := s.sendDatagramTracked(eps, true)
	return err
}

// sendDatagramTracked 发送 datagram 并返回分配的 datagram seq。
// 当 track=true 且包含 reliable EP 时把 EPs 加入 resend 队列；
// 当 track=false 时不入队（用于重传，由 retransmit 通过 Rekey 维护跟踪）。
//
// **dynamic padding to minDatagramSize**：跨境链路上某些 NAT/QoS 设备会确定性丢弃
// 特定 size 范围（实测 ~400 字节）的 UDP 包。该现象表现为"某条 small EP 重传 60 次
// 都到不了对端" — server 端读出 770KB 完整数据写入 RakNet，但 RakNet 层 maxTry 用尽
// 提前 close。把所有 datagram 用 random padding EP 补足到 minDatagramSize=800 后，
// small EP 的 wire 表现跟大 EP 一致，绕过 size-based 黑洞。
// padding EP 用 Reliability=Unreliable + IDDetectLostConnections head，对端 deliver
// silent return；body 长度随机让重传字节序列每次不同。
func (s *Session) sendDatagramTracked(eps []EncapsulatedPacket, track bool) (uint32, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	payload := EncodeEncapsulated(eps)
	const minDatagramSize = 800
	if 4+len(payload) < minDatagramSize {
		// EP body len + body itself 之外，padding EP header 是 1 byte flags + 2 byte bitlen
		// = 3 bytes（Unreliable EP 无 MsgIndex/SeqIndex/OrderIndex/Frag）。
		const padHeaderSize = 3
		need := minDatagramSize - 4 - len(payload) - padHeaderSize
		// 在需要的基础上额外加 0-32 字节随机，让重传时即使 body 大小固定，padding 总长也不同。
		extra := 0
		if r := rand.Intn(33); r > 0 {
			extra = r
		}
		pad := buildPaddingEPOfSize(need + extra)
		payload = append(payload, EncodeEncapsulated([]EncapsulatedPacket{pad})...)
	}
	seq := s.seqOut.Add(1) - 1
	seq &= 0xFFFFFF
	out := make([]byte, 0, 4+len(payload))
	out = append(out, FlagValid)
	out = appendUint24LE(out, seq)
	out = append(out, payload...)
	_, err := s.conn.WriteTo(out, s.remote)
	if err != nil {
		return seq, err
	}
	if debug.Enabled() && len(eps) > 0 {
		ep := eps[0]
		debug.Logf("send seq=%d track=%v eps=%d frag=%v fragIdx=%d/%d order=%d rel=%v msgIdx=%d bodyLen=%d head=%02x dgramSize=%d",
			seq, track, len(eps), ep.Fragmented, ep.FragIndex, ep.FragCount,
			ep.OrderIndex, ep.Reliability, ep.MsgIndex, len(ep.Body), firstByte(ep.Body), len(out))
	}
	if !track {
		return seq, nil
	}
	hasReliable := false
	for _, ep := range eps {
		if ep.Reliability.IsReliable() {
			hasReliable = true
			break
		}
	}
	if hasReliable {
		s.resend.Add(seq, eps)
	}
	return seq, nil
}

// RunBackground 后台循环：dispatch、flush ACK、超时重传、心跳保活、连接超时。
func (s *Session) RunBackground(ctx context.Context, idleTimeout time.Duration) {
	if idleTimeout == 0 {
		idleTimeout = 30 * time.Second
	}
	go s.dispatchLoop(ctx)
	ackTick := time.NewTicker(20 * time.Millisecond)
	retransTick := time.NewTicker(50 * time.Millisecond)
	pingTick := time.NewTicker(5 * time.Second)
	idleTick := time.NewTicker(1 * time.Second)
	defer ackTick.Stop()
	defer retransTick.Stop()
	defer pingTick.Stop()
	defer idleTick.Stop()
	for {
		select {
		case <-ctx.Done():
			_ = s.Close()
			return
		case <-s.stopped:
			return
		case <-ackTick.C:
			s.flushAcks()
		case <-retransTick.C:
			s.doResend()
		case <-pingTick.C:
			if s.State() == StateConnected {
				_ = s.writeEncapsulated(BuildConnectedPing(time.Now().UnixMilli()), RelUnreliable, 0)
			}
		case <-idleTick.C:
			if time.Since(time.Unix(0, s.lastRecv.Load())) > idleTimeout {
				_ = s.Close()
				return
			}
			s.frag.SweepExpired(time.Now())
		}
	}
}

func (s *Session) flushAcks() {
	seqs := s.ackCollector.Flush()
	if len(seqs) > 0 {
		records := CompactAckRanges(seqs)
		payload := EncodeAck(records)
		out := make([]byte, 0, 1+len(payload))
		out = append(out, FlagValid|FlagACK)
		out = append(out, payload...)
		_, _ = s.conn.WriteTo(out, s.remote)
	}
	// NAK fast retransmit：把刚检测到的缺失 seq 通知对端立即重传。
	// 不等 server 端 RTO（200ms），在跨境高 RTT 链路上能省下 ~150ms × N 次重传。
	miss := s.recvWindow.ClaimMissing()
	if len(miss) > 0 {
		records := CompactAckRanges(miss)
		payload := EncodeAck(records)
		out := make([]byte, 0, 1+len(payload))
		out = append(out, FlagValid|FlagNAK)
		out = append(out, payload...)
		_, _ = s.conn.WriteTo(out, s.remote)
		if debug.Enabled() {
			debug.Logf("send NAK missing=%d records=%v", len(miss), records)
		}
	}
}

func (s *Session) doResend() {
	// ResendQueue 在 maxTry 用尽后 silent drop entry — 对端会永远等那条 OrderIndex。
	// 检测到有放弃的条目时立即 close session 而不是让数据 silent corruption。
	if s.resend.DropAttempts() > s.lastDropSeen {
		s.lastDropSeen = s.resend.DropAttempts()
		if debug.Enabled() {
			debug.Logf("resend queue dropped entry (maxTry exceeded) — closing session, drops=%d",
				s.lastDropSeen)
		}
		_ = s.Close()
		return
	}
	for _, item := range s.resend.DueForResend(time.Now()) {
		s.retransmit(item)
	}
}

// retransmit 用**新**的 datagram seq 重发 item.EPs，并 Rekey resend queue
// 让对应 entry 跟踪新 seq。
//
// 关键修复：用新 datagram seq 而不是复用原 seq。某些路径性丢包
// （运营商/路由器按 5-tuple+payload hash 持续丢同一字节序列）会让原 seq
// 的重传永远到不了；新 seq 通常能避开这种确定性丢包。
// 原 EP 的 MsgIndex/FragIndex/OrderIndex 保持不变，符合 RakNet 标准。
func (s *Session) retransmit(item RetransmitItem) {
	// padding 由 sendDatagramTracked 统一处理（dynamic padding 到 minDatagramSize）。
	newSeq, err := s.sendDatagramTracked(item.EPs, false)
	if err != nil {
		if debug.Enabled() {
			debug.Logf("retransmit write err=%v", err)
		}
		return
	}
	if debug.Enabled() {
		for _, ep := range item.EPs {
			debug.Logf("retransmit oldSeq=%d newSeq=%d frag=%v fragIdx=%d/%d order=%d rel=%v msgIdx=%d bodyLen=%d head=%02x",
				item.OriginalSeq, newSeq, ep.Fragmented, ep.FragIndex, ep.FragCount,
				ep.OrderIndex, ep.Reliability, ep.MsgIndex, len(ep.Body), firstByte(ep.Body))
		}
	}
	s.resend.Rekey(item.OriginalSeq, newSeq)
}

// buildPaddingEP 构造一个随机长度的 unreliable padding EP。
// body 第一字节固定 IDDetectLostConnections（0x04）— 对端 deliver case 已 silent return；
// 后续字节随机。每次调用 body 长度也随机（8..64），保证连续重传时 datagram 字节序列不同。
//
// 使用 math/rand 包级函数（v1 内部 lockedSource，并发安全；不需要密码学强度）。
func buildPaddingEP() EncapsulatedPacket {
	const minLen, maxLen = 8, 64
	n := minLen + rand.Intn(maxLen-minLen+1)
	return buildPaddingEPOfSize(n)
}

// buildPaddingEPOfSize 构造指定 body 长度的 padding EP（min 9，保留首字节 ID）。
func buildPaddingEPOfSize(n int) EncapsulatedPacket {
	if n < 9 {
		n = 9
	}
	body := make([]byte, n)
	body[0] = IDDetectLostConnections
	_, _ = rand.Read(body[1:])
	return EncapsulatedPacket{
		Reliability: RelUnreliable,
		Body:        body,
	}
}

// NewClientSession 构造客户端 Session（不主动监听，由 Dial 拨号驱动）。
func NewClientSession(pc net.PacketConn, remote net.Addr, clientGUID int64) *Session {
	s := NewSession(pc, remote, 1492, 0)
	s.isClient = true
	s.clientGUID = clientGUID
	return s
}

// Dial 拨号一个 Bedrock RakNet 服务器。返回时 Session 已处于 Connected 状态。
// 调用方应将返回的 Session 持有，并在 ctx 取消时 Close。
func Dial(ctx context.Context, target string) (*Session, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", target)
	if err != nil {
		return nil, err
	}
	pc, err := net.ListenPacket("udp", "0.0.0.0:0")
	if err != nil {
		return nil, err
	}
	// 设置 socket buffer 到 4MB —— 跨境高 RTT + burst 时默认 ~200KB 会让内核 silently drop
	// datagram，表现为"某条 EP 反复重传到不了对端"。详见 listener/udp.go 同位置说明。
	if udpConn, ok := pc.(*net.UDPConn); ok {
		_ = udpConn.SetReadBuffer(4 * 1024 * 1024)
		_ = udpConn.SetWriteBuffer(4 * 1024 * 1024)
	}
	clientGUID := rand.Int63()
	if clientGUID == 0 {
		clientGUID = time.Now().UnixNano()
	}
	sess := NewClientSession(pc, udpAddr, clientGUID)

	// 接收 goroutine
	go func() {
		buf := make([]byte, 2048)
		for {
			n, addr, rerr := pc.ReadFrom(buf)
			if rerr != nil {
				return
			}
			if addr.String() != udpAddr.String() {
				continue
			}
			payload := make([]byte, n)
			copy(payload, buf[:n])
			_ = sess.Feed(payload)
		}
	}()
	go sess.RunBackground(ctx, 30*time.Second)
	go func() {
		<-sess.closed
		_ = pc.Close()
	}()

	// 主动握手：循环尝试不同 MTU
	mtus := []uint16{1492, 1200, 900, 576}
	for _, mtu := range mtus {
		_, _ = pc.WriteTo(BuildOpenConnectionRequest1(mtu), udpAddr)
		select {
		case <-time.After(500 * time.Millisecond):
		case <-sess.handshakeDone:
			return sess, nil
		case <-ctx.Done():
			_ = sess.Close()
			return nil, ctx.Err()
		}
	}

	// 等待握手完成
	select {
	case <-time.After(10 * time.Second):
		_ = sess.Close()
		return nil, errors.New("raknet: dial handshake timeout")
	case <-sess.handshakeDone:
		return sess, nil
	case <-ctx.Done():
		_ = sess.Close()
		return nil, ctx.Err()
	}
}

func firstByte(b []byte) byte {
	if len(b) == 0 {
		return 0
	}
	return b[0]
}

// GenServerGUID 生成一个进程级稳定的服务端 GUID。
var (
	guidOnce sync.Once
	guid     int64
)

func GenServerGUID() int64 {
	guidOnce.Do(func() {
		guid = rand.Int63()
		if guid == 0 {
			guid = time.Now().UnixNano()
		}
	})
	return guid
}
