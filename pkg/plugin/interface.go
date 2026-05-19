// Package plugin 提供拦截/钩子机制。
package plugin

import (
	"context"

	"github.com/shuffleman/mcte/pkg/protocol/java"
	"github.com/shuffleman/mcte/pkg/session"
)

// Plugin 生命周期接口。
type Plugin interface {
	Name() string
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

// PacketAction 包钩子结果。
type PacketAction int

const (
	PacketPass PacketAction = iota
	PacketModified
	PacketDrop
)

// PacketHook 入站/出站包拦截。
type PacketHook interface {
	OnPacket(s *session.Session, dir java.Direction, p *java.Packet) PacketAction
}

// SessionEvent 连接事件。
type SessionEvent int

const (
	SessionOpen SessionEvent = iota
	SessionClose
	SessionStateChange
)

// SessionHook 连接打开/关闭/状态变更回调。
type SessionHook interface {
	OnSessionEvent(s *session.Session, ev SessionEvent)
}
