# MCTE Inbound / Outbound 协议参考

MCTE 现已成为 inbound/outbound 等级的代理协议（与 VLESS、Trojan 同级），
通过 **UUID 身份认证**区分用户，伪装成 Minecraft 服务器流量。

---

## 一、协议格式

### Java (TCP, 端口 25565)

```
TCP 连接建立
  └─ Handshake 包:
       protocolVersion (VarInt)         必须为 ≥ 769 (1.21.4)
       serverAddress   (String)         "<host>\x00<uuid-string>"
       serverPort      (uint16)
       nextState       (VarInt)         2 (Login)

  └─ LoginStart 包:
       username (String)                任意值
       UUID     (16 bytes)              客户端 UUID（不参与认证）

  ← LoginPluginRequest:
       messageID (VarInt)
       channel   (String)               "mcte:negotiate"
       data      (raw)

  → LoginPluginResponse:
       messageID (VarInt)
       success   (bool)                 true
       data      (raw):
         version  (1 byte)              1
         hostLen  (uint16 BE)
         host     (string)              目标主机
         port     (uint16 BE)           目标端口

  此后 Play 阶段 PluginMessage 通道 (mcte:data) 双向承载应用层字节流。
  KeepAlive 由 MCTE 端发送，客户端必须回包 (SDK 自动处理)。
```

### Bedrock (UDP, 端口 19132, RakNet)

```
RakNet 完整握手 (UnconnectedPing/Pong, OpenConnectionRequest 1/2,
                ConnectionRequest, NewIncomingConnection)

  → Login game packet (0xFE):
       protocolVersion (BE int32)       ≥ 766 (1.21.4)
       payloadLen      (VarUint32)
       chainLen        (LE int32) + chain JSON
       cdLen           (LE int32) + clientData JWT (unsigned)
         claims:
           MCTEUuid   : "<uuid-string>"
           MCTETarget : "host:port"

  ← PlayStatus(LoginSuccess) game packet

  此后 PacketTunnelData (0x200) game packet 双向承载应用层字节流。
```

### 认证

- inbound 持有 `users: [{name, uuid, level}]` 列表
- 收到 Handshake / Login 后查 UUID
- **未知 UUID** → 静默透传到 `fallback.targets`（真实 MC server），抗主动探测

---

## 二、独立运行（standalone）

```bash
go build -o mcte ./cmd/mcte
./mcte -config config/example.yaml
```

最小 `example.yaml`:

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

---

## 三、作为 Xray inbound/outbound

### 引入依赖

在 Xray fork 的 `go.mod` 添加：

```
require github.com/shuffleman/mcte/pkg/integration/xray v0.0.0
replace  github.com/shuffleman/mcte/pkg/integration/xray => /path/to/mcte/pkg/integration/xray
```

### Inbound 注册（伪代码）

```go
import mcteX "github.com/shuffleman/mcte/pkg/integration/xray"

func init() {
    common.Must(common.RegisterConfig((*MCTEInbound)(nil), func(ctx context.Context, c interface{}) (interface{}, error) {
        cfg := c.(*MCTEInbound).toMCTEConfig()
        return mcteX.NewServer(cfg, logger, &xrayDispatcher{routing.DispatcherFromContext(ctx)})
    }))
}

type xrayDispatcher struct{ d routing.Dispatcher }
func (a *xrayDispatcher) Dispatch(ctx context.Context, conn net.Conn, host string, port uint16) error {
    link, err := a.d.Dispatch(ctx, net.Destination{
        Network: net.Network_TCP,
        Address: net.ParseAddress(host),
        Port:    net.Port(port),
    })
    if err != nil { return err }
    // 桥接 conn <-> link
    ...
}
```

### Outbound 注册

```go
common.Must(common.RegisterConfig((*MCTEOutbound)(nil), func(ctx context.Context, c interface{}) (interface{}, error) {
    cfg := c.(*MCTEOutbound).toClientConfig()
    return mcteX.NewClient(cfg, logger)
}))
```

Xray Outbound.Process 内部：
```go
ob := session.OutboundFromContext(ctx)
host := ob.Target.Address.String()
return o.client.Process(ctx, link.Reader, link.Writer, host, uint16(ob.Target.Port))
```

### JSON 配置示例

```jsonc
// inbound
{
  "tag": "in-mcte",
  "protocol": "minecraft",
  "settings": {
    "users": [{"uuid": "550e8400-...", "name": "alice"}],
    "fallback": ["127.0.0.1:25566"],
    "listenTcp": "0.0.0.0:25565",
    "listenUdp": "0.0.0.0:19132"
  }
}

// outbound
{
  "tag": "out-mcte",
  "protocol": "minecraft",
  "settings": {
    "server": "mcte.example.com",
    "serverPort": 25565,
    "uuid": "550e8400-...",
    "network": "tcp"
  }
}
```

---

## 四、作为 sing-box inbound/outbound

### Inbound

```go
import mcteSB "github.com/shuffleman/mcte/pkg/integration/singbox"

inbound.Register[mcteSB.Options](registry, "minecraft", func(ctx, router, logger, tag, opts) (adapter.Inbound, error) {
    return mcteSB.New(opts, logger, &sbRouteAdapter{router})
})

type sbRouteAdapter struct{ r adapter.ConnectionRouterEx }
func (a *sbRouteAdapter) RouteConnection(ctx context.Context, c net.Conn, host string, port uint16) error {
    a.r.RouteConnectionEx(ctx, c, adapter.InboundContext{
        Destination: M.ParseSocksaddrHostPort(host, port),
    }, nil)
    return nil
}
```

### Outbound

```go
outbound.Register[mcteSB.OutboundOptions](registry, "minecraft", func(ctx, router, log, tag, opts) (adapter.Outbound, error) {
    impl, err := mcteSB.NewOutbound(opts, log)
    if err != nil { return nil, err }
    return &mcteSBOutbound{
        Adapter: outbound.NewAdapter("minecraft", tag, []string{"tcp", "udp"}),
        impl:    impl,
    }, nil
})

func (h *mcteSBOutbound) DialContext(ctx context.Context, network string, dest M.Socksaddr) (net.Conn, error) {
    return h.impl.DialContext(ctx, dest.AddrString(), uint16(dest.Port))
}
```

### JSON 配置示例

```jsonc
// inbound
{
  "type": "minecraft",
  "tag": "in-mcte",
  "listen": "0.0.0.0:25565",
  "listen_udp": "0.0.0.0:19132",
  "users": [{"uuid": "550e8400-...", "name": "alice"}],
  "fallback": ["127.0.0.1:25566"]
}

// outbound
{
  "type": "minecraft",
  "tag": "out-mcte",
  "server": "mcte.example.com",
  "server_port": 25565,
  "uuid": "550e8400-...",
  "network": "tcp"
}
```

---

## 五、客户端 SDK 直接使用

```go
import "github.com/shuffleman/mcte/pkg/client"

c, _ := client.New(client.Config{
    Server:  "mcte.example.com",
    Port:    25565,
    UUID:    "550e8400-e29b-41d4-a716-446655440000",
    Network: "tcp",          // 或 "udp" (Bedrock)
})
conn, err := c.Dial(ctx, "target.host", 8080)
// 当作普通 net.Conn 读写
```

---

## 六、抗探测说明

| 探测方式 | 效果 |
|---|---|
| 端口扫描 (无 UUID) | 看到完整 MC 服务器响应 → fallback 透传 |
| 错误 UUID | 静默透传到 fallback，与未知客户端等价 |
| 正版认证扫描 | MCTE 不参与 Mojang 认证；fallback 行为决定 |
| 流量画像 | 大流量隧道时仍像 MC PluginMessage / Bedrock game packet |

---

## 七、设计要点（与 sing-box VLESS 对比）

| 维度 | VLESS+WebSocket | MCTE |
|---|---|---|
| 伪装载体 | WebSocket / TLS | Minecraft 协议 |
| 身份认证 | UUID + Flow | UUID |
| 多用户支持 | ✓ | ✓ |
| TCP / UDP 都支持 | ✓ (transport 控制) | ✓ (Java=TCP, Bedrock=UDP) |
| 抗 active probe | TLS + WS path | UUID 错误降级真 MC server |
| 流控 | TCP 内核 | TCP 内核 + Bedrock cwnd (slow start + AIMD) |
| Watchdog | TCP keepalive + sing 包装 deadline | WatchdogConn 30s 多 writer 安全 |
