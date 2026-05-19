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
	resend    *ResendQueue
	writeMu   sync.Mutex

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
		recvCh:        make(chan []byte, 256),
		inbox:         make(chan []byte, 512),
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
func (s *Session) ReadApp(ctx context.Context) ([]byte, error) {
	select {
	case b, ok := <-s.recvCh:
		if !ok {
			return nil, net.ErrClosed
		}
		return b, nil
	case <-s.closed:
		return nil, net.ErrClosed
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

// Close 关闭 session 并发出 Disconnect 通知。
func (s *Session) Close() error {
	s.stopOnce.Do(func() {
		// 尽力发一个 disconnect notification
		_ = s.writeEncapsulated([]byte{IDDisconnectNotification}, RelReliableOrdered, 0)
		close(s.stopped)
		s.state.Store(int32(StateClosed))
		close(s.closed)
	})
	return nil
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
		s.dropInbox.Add(1)
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
		for _, p := range s.resend.NakRange(records) {
			_, _ = s.conn.WriteTo(p, s.remote)
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
		s.flushAcks() // 容量已满，立刻发出
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
			for _, queued := range s.ordering[ch].Push(logical) {
				s.deliver(queued.Body)
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
	for i := 0; i < count; i++ {
		start := i * s.maxFrag
		end := start + s.maxFrag
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

func (s *Session) sendDatagram(eps []EncapsulatedPacket) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	payload := EncodeEncapsulated(eps)
	seq := s.seqOut.Add(1) - 1
	seq &= 0xFFFFFF
	out := make([]byte, 0, 4+len(payload))
	out = append(out, FlagValid)
	out = appendUint24LE(out, seq)
	out = append(out, payload...)
	_, err := s.conn.WriteTo(out, s.remote)
	if err != nil {
		return err
	}
	hasReliable := false
	for _, ep := range eps {
		if ep.Reliability.IsReliable() {
			hasReliable = true
			break
		}
	}
	if hasReliable {
		s.resend.Add(seq, out)
	}
	return nil
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
	if len(seqs) == 0 {
		return
	}
	records := CompactAckRanges(seqs)
	payload := EncodeAck(records)
	out := make([]byte, 0, 1+len(payload))
	out = append(out, FlagValid|FlagACK)
	out = append(out, payload...)
	_, _ = s.conn.WriteTo(out, s.remote)
}

func (s *Session) doResend() {
	for _, p := range s.resend.DueForResend(time.Now()) {
		_, _ = s.conn.WriteTo(p, s.remote)
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
