// Package detector 识别连接类型并按需返回可重读的 conn。
package detector

import (
	"strings"

	"github.com/google/uuid"
)

// ExtractUUIDFromAddr 从 Java Handshake 的 serverAddress 字段中提取附加 UUID。
//
// 协议形态：`<host>\x00<uuid>`（兼容 BungeeCord 习惯）。
// 真实 MC 客户端：`serverAddress = host`（无 \x00），返回 host, uuid.Nil。
// MCTE 客户端：末段是 36 字符 canonical UUID 或 32 字符 hex。
//
// 容错：若末段不是合法 UUID 则返回 uuid.Nil，调用方应当作真实 MC 客户端走 fallback。
func ExtractUUIDFromAddr(serverAddr string) (host string, id uuid.UUID) {
	parts := strings.Split(serverAddr, "\x00")
	host = parts[0]
	if len(parts) < 2 {
		return host, uuid.Nil
	}
	for i := len(parts) - 1; i >= 1; i-- {
		cand := strings.TrimSpace(parts[i])
		if cand == "" {
			continue
		}
		if u, err := uuid.Parse(cand); err == nil {
			return host, u
		}
	}
	return host, uuid.Nil
}

// EncodeAddrWithUUID 用作 outbound 客户端：把 UUID 拼到 serverAddress。
func EncodeAddrWithUUID(host string, id uuid.UUID) string {
	if id == uuid.Nil {
		return host
	}
	return host + "\x00" + id.String()
}
