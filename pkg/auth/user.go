// Package auth 实现 UUID 身份认证与多用户管理。
//
// MCTE 作为 inbound/outbound 协议，使用 UUID 鉴权：
//   - inbound 持有 UUID → User 映射；
//   - 客户端在握手时把 UUID 嵌入伪装载体（Java 是 handshake serverAddress 末段，
//     Bedrock 是 LoginPacket.clientData JWT claim 中的 MCTEUuid）；
//   - 未知 UUID / 缺失 UUID 的连接 → 透传到 fallback（保持与真实 MC 完全一致的行为）。
//
// 这是 VLESS 风格的"明文 UUID"认证，简洁优先，依靠 fallback 等价提供抗主动探测能力。
package auth

import (
	"errors"
	"strings"
	"sync"

	"github.com/google/uuid"
)

// User 表示一个 MCTE 用户。
type User struct {
	Name  string    // 业务名（日志用，可为空，缺省取 UUID 字符串）
	UUID  uuid.UUID // 16 字节 UUID
	Level int32     // 流量等级（保留，供路由层使用）
}

// String 返回可读名称。
func (u *User) String() string {
	if u.Name != "" {
		return u.Name
	}
	return u.UUID.String()
}

// Validator 持有 UUID → User 映射。
// 设计点：内部用 sync.RWMutex，AddUser/RemoveUser 偶发；Lookup 高频读，少竞争。
type Validator struct {
	mu    sync.RWMutex
	users map[uuid.UUID]*User
}

// NewValidator 构造。
func NewValidator() *Validator {
	return &Validator{users: make(map[uuid.UUID]*User)}
}

// Add 添加用户；重复 UUID 返回错误。
func (v *Validator) Add(u *User) error {
	if u == nil {
		return errors.New("auth: nil user")
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if _, ok := v.users[u.UUID]; ok {
		return errors.New("auth: duplicate UUID: " + u.UUID.String())
	}
	v.users[u.UUID] = u
	return nil
}

// Remove 按 UUID 移除。
func (v *Validator) Remove(id uuid.UUID) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	if _, ok := v.users[id]; ok {
		delete(v.users, id)
		return true
	}
	return false
}

// Lookup 按 UUID 查找；找不到返回 nil。
func (v *Validator) Lookup(id uuid.UUID) *User {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.users[id]
}

// Count 当前用户数。
func (v *Validator) Count() int {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return len(v.users)
}

// ParseUUID 容错地解析 UUID 字符串。
// 支持 36 字符 canonical (`xxxxxxxx-xxxx-...`)、32 字符无破折号、不区分大小写。
func ParseUUID(s string) (uuid.UUID, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return uuid.Nil, errors.New("auth: empty uuid")
	}
	return uuid.Parse(s)
}
