// Package session 维护连接会话。
package session

import (
	"net"
	"sync/atomic"
	"time"

	"github.com/shuffleman/mcte/pkg/detector"
)

// FSMState session 生命周期状态。
type FSMState int32

const (
	FSMNew FSMState = iota
	FSMHandshake
	FSMNegotiate
	FSMPlay
	FSMBridging
	FSMClosed
)

// Session 包含一条连接的全部上下文。
type Session struct {
	ID          uint64
	Kind        detector.ClientKind
	Proto       detector.ProtocolType
	Version     int32
	RemoteAddr  net.Addr
	CreatedAt   time.Time
	lastActive  atomic.Int64 // unix nano
	state       atomic.Int32
}

func (s *Session) Touch()          { s.lastActive.Store(time.Now().UnixNano()) }
func (s *Session) LastActive() time.Time {
	return time.Unix(0, s.lastActive.Load())
}
func (s *Session) State() FSMState   { return FSMState(s.state.Load()) }
func (s *Session) SetState(v FSMState) { s.state.Store(int32(v)) }

// Reset 在归还到池前清零字段。
func (s *Session) Reset() {
	s.ID = 0
	s.Kind = 0
	s.Proto = 0
	s.Version = 0
	s.RemoteAddr = nil
	s.CreatedAt = time.Time{}
	s.lastActive.Store(0)
	s.state.Store(int32(FSMNew))
}
