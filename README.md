# MCTE — Minecraft Transport Engine

基于 Minecraft 协议的 inbound/outbound 等级代理协议，Go 实现。

- **UUID 身份认证** + 多用户管理（VLESS 风格）
- **同时支持 TCP (Java 25565) 与 UDP/RakNet (Bedrock 19132)**
- **抗主动探测**：未认证流量静默透传到真实 MC server，与正常 MC 流量完全无法区分
- **可作为独立守护进程运行**，也可接入 Xray inbound/outbound 或 sing-box inbound/outbound

---

## 快速开始

### 独立运行

```bash
go build -o mcte ./cmd/mcte
./mcte -config config/example.yaml
```

最小配置：

```yaml
listen:
  tcp: "0.0.0.0:25565"
  udp: "0.0.0.0:19132"

users:
  - name: alice
    uuid: "550e8400-e29b-41d4-a716-446655440000"

fallback:
  targets:
    - "127.0.0.1:25566"   # 真实 MC server
```

### 作为客户端（outbound SDK）

```go
import "github.com/shuffleman/mcte/pkg/client"

c, _ := client.New(client.Config{
    Server: "mcte.example.com", Port: 25565,
    UUID: "550e8400-...", Network: "tcp",
})
conn, _ := c.Dial(ctx, "target.host", 8080)
// conn 是普通 net.Conn
```

### 接入 Xray / sing-box

见 `docs/INBOUND_OUTBOUND.md` 和 `DELIVERABLES.md`。

---

## 项目状态

```
go build ./...    exit 0  (主仓 + integration/xray + integration/singbox)
go vet ./...      exit 0
go test ./...     ok 45/45 PASS
```

详细交付文档：[DELIVERABLES.md](./DELIVERABLES.md)
集成手册：[docs/INBOUND_OUTBOUND.md](./docs/INBOUND_OUTBOUND.md)
原始需求：[MCTE_DEV_PLAN.md](./MCTE_DEV_PLAN.md)
