# MCTE — Minecraft Transport Engine
## 完整交付文档

> 文档版本：v1.1（2026-05-19）
> 项目状态：inbound/outbound 等级代理协议 + UUID 身份认证 + 流量画像伪装
> 构建结果：3 个 Go module 全部 build 通过、vet 无警告、**52/52** 测试 PASS

---

## 目录

1. [项目概览](#1-项目概览)
2. [协议规范](#2-协议规范)
3. [流量画像伪装](#3-流量画像伪装)
4. [架构总览](#4-架构总览)
5. [目录结构](#5-目录结构)
6. [模块详解](#6-模块详解)
7. [配置参考](#7-配置参考)
8. [部署方式](#8-部署方式)
9. [Xray 集成](#9-xray-集成)
10. [sing-box 集成](#10-sing-box-集成)
11. [客户端 SDK](#11-客户端-sdk)
12. [背压与流控](#12-背压与流控)
13. [断开事件传递](#13-断开事件传递)
14. [防爆设计](#14-防爆设计)
15. [安全特性](#15-安全特性)
16. [测试覆盖](#16-测试覆盖)
17. [关键约束](#17-关键约束)
18. [已知限制](#18-已知限制)

---

## 1. 项目概览

### 1.1 定位

MCTE 是 inbound/outbound 等级的代理协议（与 VLESS、Trojan 同级），具备：

- **UUID 身份认证** + 多用户管理
- **TCP + UDP 双承载**：Java MC (TCP 25565) 与 Bedrock MC (UDP/RakNet 19132) 同时支持
- **流量画像伪装**：客户端按数据大小映射到不同"玩家动作" channel（移动 / 飞行 / 区块加载 / 空闲心跳）
- **抗主动探测**：未知 UUID 静默透传到真实 MC server，与正常 MC 流量完全无法区分
- **可作为独立守护进程运行**，也可作为 Xray inbound/outbound 或 sing-box inbound/outbound 接入路由层

### 1.2 协议范围

| 维度 | 支持 |
|---|---|
| Minecraft Java Edition | 1.21.4 / 1.21.5 / 1.21.6 / 1.21.8 (proto 769-772) |
| Minecraft Bedrock Edition | ≥ 1.21.4 (proto ≥ 766) |
| Bedrock 网络层 | 完整 RakNet：离线/在线握手、ACK/NAK、重传、分片、ordered channel、CC |
| Java 协议层 | Handshake / Login / Configuration / Play 完整子集 |

### 1.3 关键技术指标

| 指标 | 值 |
|---|---|
| 单 Bedrock session 内存上限 | ~4 MB（cwnd × MTU + 缓冲） |
| 最大并发会话 | 默认 10000，可配 |
| RakNet 拥塞控制 | slow start + AIMD，cwnd 范围 4 - 2048 |
| 半开连接检测 | 30 秒（WatchdogConn） |
| 流量画像 idle 心跳 | 默认 1.5 秒（≈ 30 tick） |
| Bedrock 离线 ping 响应 | listener 主路径同步回包，无 session 占用 |
| Java framer | 无锁写，单次 Write 由内核 fd.writeLock 保证原子 |
| UUID 错误降级 | 完全等价真实 MC 服务器响应 |

---

## 2. 协议规范

### 2.1 Java (TCP 25565)

```
[TCP 连接建立]

→ Handshake packet (id=0x00):
    protocolVersion : VarInt           (≥ 769)
    serverAddress   : String           "<host>\x00<uuid-canonical>"
    serverPort      : uint16
    nextState       : VarInt           (2 = Login)

→ LoginStart packet:
    username : String
    uuid     : 16 bytes                (客户端 UUID，仅信息用)

← LoginPluginRequest packet:
    messageID : VarInt
    channel   : String                 "mcte:negotiate"
    data      : empty

→ LoginPluginResponse packet:
    messageID  : VarInt
    successful : bool                  true
    data       : raw
        version  : uint8               (= 1)
        hostLen  : uint16 BE
        host     : UTF-8 字符串
        port     : uint16 BE

[此后进入 Play 阶段]
  · 双向 PluginMessage 承载应用层字节流：
      兼容模式：单一 channel = "mcte:data"
      画像模式：mcte:m / mcte:f / mcte:c / mcte:i（按数据大小动态选择）
  · MCTE 端发 KeepAlive(20s)，客户端必须回包；30s 无响应断开
```

### 2.2 Bedrock (UDP 19132, RakNet)

```
[RakNet 完整握手]
  UnconnectedPing  ↔  UnconnectedPong
  OpenConnectionRequest1  ↔  OpenConnectionReply1
  OpenConnectionRequest2  ↔  OpenConnectionReply2
  ConnectionRequest  ↔  ConnectionRequestAccepted
  NewIncomingConnection
  [Connected]

→ Game packet (0xFE) 承载 Login packet:
    headerVarUint32 : packetID = 0x01
    protocolVersion : BE int32        (≥ 766)
    payloadLen      : VarUint32
    inner:
        chainLen   : LE int32
        chainJSON  : { "chain": ["<jwt>","..."] }
        cdLen      : LE int32
        cdJWT      : unsigned JWT
            claims:
                MCTEUuid   : "<uuid-canonical>"
                MCTETarget : "host:port"

← Game packet (0xFE) 承载 PlayStatus(LoginSuccess):
    status : BE int32 = 0

[此后双向]
  → Game packet 承载 PacketTunnelData (id=0x200):
        payload = 应用层字节
  ← Game packet 承载 PacketTunnelData
```

### 2.3 认证逻辑

```
inbound 收到 handshake → 提取 UUID
  ├─ 缺失 / 解析失败 → KindMC → fallback 透传
  ├─ 不在 users 列表 → KindMC → fallback 透传（抗探测）
  └─ 在列表 → KindTunnel → 隧道路径（res.User 携带用户信息）
```

**关键不变量**：未认证流量行为与真实 MC 完全一致（透传），任何对外可观察行为都不能泄漏"这是 MCTE 端口"。

---

## 3. 流量画像伪装

### 3.1 设计动机

即使隧道认证完美，**包大小分布**仍能暴露代理特征：
- 单一 channel + 单一大小分布 → 与真实 MC 玩家分布（移动 ~28B + 偶发大包）显著不同
- 长时间无包传输 → 真实玩家持续输入产生稳定背景流量
- 大文件传输 → 与真实 PluginMessage 边界不符

### 3.2 画像方案

客户端 `MimicProfile` 按数据大小映射到不同"玩家动作" channel：

| Action | 数据大小 | channel | 模拟的真实 MC 包 |
|---|---|---|---|
| `ActionMove` | ≤ 32 B | `mcte:m` | ServerboundMovePlayerPos (~28B) |
| `ActionFly` | 32 B < size ≤ 256 B | `mcte:f` | ServerboundMovePlayerPosRot + 输入/聊天 |
| `ActionChunk` | > 256 B | `mcte:c` | ClientboundLevelChunkWithLight（按 2KB 分片） |
| `ActionIdle` | 空载荷心跳 | `mcte:i` | ClientboundTickEnd 等持续输入 |

### 3.3 双向画像

- **上行（客户端 → MCTE）**：`Pack(data)` 按阈值切片成多 chunk，每 chunk 选不同 channel
- **下行（MCTE → 客户端）**：inbound bridge 用同样阈值反向切片，让 server→client 也呈现玩家行为分布
- **空闲心跳**：客户端独立 goroutine，距上次 Write ≥ HeartbeatInterval 发空 `mcte:i` 包

### 3.4 inbound 识别

inbound `MimicMatcher`：
- 任何以 `Prefix`（默认 `mcte:`）开头的 PluginMessage 都视为隧道流量
- `mcte:i` 数据被丢弃（心跳）
- 其余 mcte:* channel 的 payload 顺序转发到上游 → 字节完整性保证

### 3.5 字节完整性

`MimicProfile.Pack()` 严格按输入顺序切片，inbound bridge 按收到顺序写入 upstream，所以**应用层数据保持完全有序、无截断**。`TestMimicRoundtrip` 与 `TestMimicEndToEnd` 验证。

### 3.6 启用方式

**客户端**：

```go
sdk, _ := client.New(client.Config{
    Server: "...", Port: 25565, UUID: "...",
    Mimic: client.DefaultProfile(),
})
```

**inbound (YAML)**：

```yaml
tunnel:
  mimic:
    enabled: true
    # 默认值即可与 client.DefaultProfile 配套
```

可自定义阈值，但**两端必须一致**（否则 inbound 识别不出客户端发送的 channel）。

---

## 4. 架构总览

```
┌──────────────────────────────────────────────────────────────┐
│                         MCTE Engine                          │
│                                                              │
│  ┌────────────────┐         ┌────────────────┐               │
│  │  TCP Listener  │         │  UDP Listener  │               │
│  │  (Java 25565)  │         │ (Bedrock 19132)│               │
│  │  + KeepAlive   │         │  + 半开短超时  │               │
│  └───────┬────────┘         └────────┬───────┘               │
│          │ accept                    │ OCR1                  │
│          ▼                           ▼                       │
│  ┌────────────────┐         ┌────────────────┐               │
│  │ engine.handleTCP│        │ engine.handleUDP│              │
│  │ sem 限流        │        │ sem 限流        │              │
│  └───────┬────────┘         └────────┬───────┘               │
│          │                           │                       │
│          ▼                           ▼                       │
│  ┌─────────────────────────────────────────┐                 │
│  │           Detector (Java/Bedrock)       │                 │
│  │  TeeReader → Handshake → UUID 提取      │                 │
│  │  Validator.Lookup(UUID)                 │                 │
│  └────────────┬─────────────┬──────────────┘                 │
│               │             │                                │
│       KindTunnel        KindMC                               │
│               │             │                                │
│               ▼             ▼                                │
│   ┌──────────────────┐  ┌──────────────────┐                 │
│   │ Tunnel Handler   │  │ Passthrough      │                 │
│   │  · Watchdog 30s  │  │  · TCP keepalive │                 │
│   │  · KeepAlive go  │  │  · netutil.Pipe  │                 │
│   │  · Bridge 双向   │  │  · 双向 io.Copy  │                 │
│   │  · MimicMatcher  │  │  · close 传递    │                 │
│   │  · close 传递    │  │                  │                 │
│   └────────┬─────────┘  └────────┬─────────┘                 │
│            │ negotiate          │ dial backend               │
│            ▼                    ▼                            │
│   UpstreamDialer            FallbackDialer                   │
│   (engine.New 注入)         (真实 MC server)                 │
└──────────────────────────────────────────────────────────────┘

UpstreamDialer 实现：
  · DefaultUpstreamDialer    standalone 模式：直接 TCP dial
  · xrayUpstream             Xray 集成：net.Pipe + Xray dispatcher
  · singUpstream             sing-box 集成：net.Pipe + sing-box router

Client SDK：
  · client.New(Config{Mimic: DefaultProfile()})
  · Java path:   Handshake (UUID 嵌入 serverAddress) → Plugin 协商 → bridge + 心跳 goroutine
  · Bedrock path: RakNet Dial → Login (JWT claim 含 UUID/Target) → PlayStatus → tunnel data
```

---

## 5. 目录结构

```
mcte/
├── cmd/mcte/main.go                    独立进程入口
├── go.mod                              主 module
├── README.md
├── DELIVERABLES.md                     本文档
├── MCTE_DEV_PLAN.md                    原始需求规划
├── docs/INBOUND_OUTBOUND.md            inbound/outbound 集成手册
├── config/example.yaml                 完整示例配置
│
├── internal/netutil/                   网络辅助工具
│   ├── pipe.go                         双向 io.Copy + closeBoth
│   ├── timeout_conn.go                 简单 deadline 包装（单 writer 场景）
│   └── watchdog_conn.go                多 writer 场景的 Write 卡死检测
│
├── pkg/
│   ├── auth/                           身份认证
│   │   └── user.go                     User + Validator (sync.RWMutex)
│   │
│   ├── client/                         outbound SDK
│   │   ├── client.go                   主入口 + Java 拨号 + Config 含 Mimic
│   │   ├── java_bridge.go              Java net.Conn 适配 + Mimic Pack + 心跳 goroutine
│   │   ├── bedrock.go                  Bedrock 拨号
│   │   ├── bedrock_bridge.go           Bedrock net.Conn 适配
│   │   └── mimic.go                    MimicProfile + Pack/Heartbeat
│   │
│   ├── config/                         配置定义与加载
│   │   ├── config.go                   全局配置 + MimicConfig
│   │   └── loader.go                   YAML 解析与校验
│   │
│   ├── detector/                       连接识别
│   │   ├── detector.go                 公共入口
│   │   ├── token.go                    UUID 提取/编码
│   │   ├── restored_conn.go            TeeReader + raw conn 拼接
│   │   ├── java.go                     Java handshake 识别
│   │   └── bedrock.go                  Bedrock Login JWT 识别
│   │
│   ├── engine/engine.go                根引擎 + buildMimicMatcher
│   │
│   ├── listener/                       监听器
│   │   ├── tcp.go                      TCP accept (TCP keepalive 30s)
│   │   ├── udp.go                      UDP + RakNet session 多路复用
│   │   └── hybrid.go                   TCP + UDP 组合
│   │
│   ├── memory/                         内存工具
│   │   ├── pool.go                     8 级分级字节池
│   │   └── ring.go                     无锁 SPSC 环形缓冲
│   │
│   ├── passthrough/                    透传 handler
│   │   ├── dialer.go                   TCP 后端拨号（keepalive）
│   │   ├── handler.go                  Java passthrough + WatchdogConn 双端
│   │   ├── bedrock.go                  Bedrock passthrough
│   │   └── bedrock_helper.go           批包组装辅助
│   │
│   ├── plugin/                         插件接口
│   │   ├── interface.go
│   │   ├── registry.go
│   │   ├── hooks.go
│   │   └── builtin/
│   │       ├── metrics/plugin.go       Prometheus 指标
│   │       └── ratelimit/plugin.go     IP 限速 (LRU+GC)
│   │
│   ├── protocol/
│   │   ├── java/                       Java MC 协议（10 文件）
│   │   └── bedrock/
│   │       ├── packet.go / batch.go / login.go / play_status.go
│   │       └── raknet/                 完整 RakNet (8 文件)
│   │
│   ├── scheduler/                      调度（tick/packet/shaper）
│   ├── session/                        Session 生命周期（FSM + sweeper + pool）
│   │
│   ├── tunnel/                         隧道 handler
│   │   ├── handler.go                  Java 入口 + WatchdogConn + Mimic 配置
│   │   ├── bridge.go                   双向桥接 + MimicMatcher 前缀识别 + 下行切片
│   │   ├── negotiate.go                Login Plugin 协商目标
│   │   ├── virtual_play.go             最小 Play 状态包序列
│   │   ├── keepalive.go                KeepAlive 独立 goroutine
│   │   └── bedrock.go                  Bedrock 隧道 handler
│   │
│   └── integration/
│       ├── xray/                       (独立 go.mod, 5 文件)
│       └── singbox/                    (独立 go.mod, 5 文件)
│
└── tests/                              52 个测试，全部 PASS
    ├── antiflood_test.go               5 — Fragment / Ratelimit 防爆
    ├── backpressure_test.go            5 — Resend cwnd / AckCollector / OrderingBuffer
    ├── bedrock_backpressure_test.go    1 — deliver 阻塞反压
    ├── bedrock_test.go                 4 — VarUint / 批包 / PlayStatus
    ├── close_propagation_test.go       2 — Watchdog Pipe close 传递
    ├── detector_java_test.go           4 — UUID 识别 / 降级
    ├── end_to_end_test.go              1 — Java inbound + outbound 全链路
    ├── framer_concurrent_test.go       1 — 32 goroutine × 50 包并发写
    ├── framer_test.go                  3 — roundtrip / truncated / too-large
    ├── mimic_end_to_end_test.go        1 — Mimic 模式 inbound + outbound 全链路
    ├── mimic_test.go                   6 — Mimic Pack / 心跳 / channel 匹配
    ├── raknet_test.go                  5 — RakNet 编解码
    ├── restored_conn_test.go           1 — 完整重读
    ├── reverse_backpressure_test.go    2 — TimeoutConn 写超时
    ├── token_test.go                   5 — UUID / Validator
    ├── varint_test.go                  3 — VarInt 边界
    └── watchdog_test.go                3 — 单卡 / 卡死中混入正常 / 不误关
```

---

## 6. 模块详解

### 6.1 auth — 身份认证

`User{Name, UUID, Level}`；`Validator` 用 `sync.RWMutex` 包装 `map[uuid.UUID]*User`。Lookup 高频读路径无写竞争。

### 6.2 detector — 连接识别

- `RestoredConn`：TeeReader 已读字节与原 conn 通过 `io.MultiReader` 拼接。
- `DetectJava` / `DetectBedrock`：提取 UUID、查 validator、降级到 KindMC。

### 6.3 client — outbound SDK

| 文件 | 责任 |
|---|---|
| `client.go` | `New(Config)` 构造；`Dial(ctx, host, port)` 入口；按 Network 路由 |
| `java_bridge.go` | Java net.Conn 适配；Mimic 模式 Write 走 Pack；后台心跳 goroutine |
| `bedrock.go` + `bedrock_bridge.go` | Bedrock 拨号 + net.Conn 适配（PacketTunnelData 包装） |
| **`mimic.go`** | **MimicProfile + Pack + Heartbeat + IsTunnelChannel/IsIdleChannel** |

### 6.4 protocol/java — Java 协议

- `varint.go`：VarInt/VarLong + 零拷贝 Append
- `packet.go`：Reader/Writer 原语
- `framer.go`：**无锁** vectorised write（单次 Write 由 TCP fd.writeLock 保证原子）
- `versions.go`：1.21.4-1.21.8 包 ID 单表（禁止硬编码）
- `handshake.go` / `login.go` / `configuration.go` / `play.go`：协议子集

### 6.5 protocol/bedrock + raknet — Bedrock 协议

完整 RakNet（offline / online / reliability / ack / fragment / resend / window / session）+ Bedrock 包层（packet / batch / login / play_status）。

### 6.6 tunnel — 隧道 handler

| 文件 | 内容 |
|---|---|
| `handler.go` | Java 入口：WatchdogConn 包 client；handshake → Login → 协商 → virtual_play → Bridge；按配置选 `NewBridge` 或 `NewBridgeWithMimic` |
| **`bridge.go`** | **双向桥接 + MimicMatcher**：客户端方向用前缀识别多 channel；上游方向按 size 阈值切片回填多 channel |
| `negotiate.go` | Login Plugin 协商格式 + 编码 |
| `virtual_play.go` | 最小 Play 状态包序列 |
| `keepalive.go` | 独立 goroutine；客户端 30s 无响应 cancel ctx |
| `bedrock.go` | Bedrock 隧道 handler |

### 6.7 passthrough — 透传 handler

| 文件 | 内容 |
|---|---|
| `dialer.go` | TCP 后端拨号，30s TCP keepalive |
| `handler.go` | Java：两端均 WatchdogConn(30s)，netutil.Pipe 双向 + closeBoth |
| `bedrock.go` | Bedrock：raknet.Dial + 应用层 packet 对等转发 |

### 6.8 engine — 根对象

- 组装 listener / handlers / validator / manager / plugins
- `Run(ctx)`：accept → sem 限流 → 路由到 handleTCP/handleUDP
- 新增 `buildMimicMatcher(MimicConfig)`：从 yaml 构造 tunnel.MimicMatcher（带默认值兜底）

### 6.9 listener / session / scheduler / memory / plugin

- listener：TCP keepalive 30s；UDP 多路复用 + UnconnectedPing 主路径回包 + OCR1 触发 Session（max_sessions + half-open 10s）
- session：16 shard sync.Map + 30s 周期 sweeper + sync.Pool 复用
- scheduler：20TPS Ticker + 优先级队列 + token bucket
- memory：8 级分级字节池 + 无锁 SPSC ring
- plugin：三接口 + 内置 metrics / ratelimit

---

## 7. 配置参考

完整 `config/example.yaml`：

```yaml
listen:
  tcp: "0.0.0.0:25565"    # Java；留空跳过
  udp: "0.0.0.0:19132"    # Bedrock；留空跳过

tunnel:
  channel: "mcte:data"            # 兼容模式单 channel（mimic.enabled=false 时使用）
  uuid_field: "MCTEUuid"          # Bedrock JWT claim 字段
  target_field: "MCTETarget"
  write_timeout: 30s              # 任一端 Write 卡死 → 强 Close
  keepalive_every: 20s            # Java Play 阶段 KeepAlive 周期
  keepalive_timeout: 30s          # 客户端无响应超时

  mimic:                          # 流量画像（推荐启用）
    enabled: true
    prefix: "mcte:"               # 所有伪装 channel 前缀
    move_suffix: "m"              # 完整 channel = prefix + suffix
    fly_suffix: "f"
    chunk_suffix: "c"
    idle_suffix: "i"
    move_max: 32                  # ≤ 32B → mcte:m（模拟移动）
    fly_max: 256                  # ≤ 256B → mcte:f（模拟飞行/输入）
    chunk_split: 2048             # > 256B → mcte:c 按 2KB 切片（模拟区块）

users:
  - name: alice
    uuid: "550e8400-e29b-41d4-a716-446655440000"
    level: 0
  - name: bob
    uuid: "13b25c91-f5e6-4f3b-b9b4-1a2f30b76e2c"

fallback:
  targets:
    - "127.0.0.1:25566"          # 真实 MC server；多个则轮询
  dial_timeout: 5s

session:
  max_concurrent: 10000           # accept 总并发上限
  idle_timeout: 5m

scheduler:
  tick_rate: 20
  send_budget_per_tick: 65536

log:
  level: info                     # debug/info/warn/error
  format: console                 # console/json

metrics:
  enabled: false
  listen: "127.0.0.1:9090"        # Prometheus /metrics
```

### 启动期校验

- `users` 非空
- 每个 user.uuid 必须可解析
- `fallback.targets` 非空
- `listen.tcp` 与 `listen.udp` 至少一个非空

### Mimic 客户端/服务端一致性

启用画像时，**客户端 MimicProfile 与 inbound tunnel.mimic 的阈值必须一致**：
- `prefix` / `move_suffix` / `fly_suffix` / `chunk_suffix` / `idle_suffix` 完全相同
- `move_max` / `fly_max` 推荐相同（不同也能跑，但下行画像不对称）
- `chunk_split` 与客户端 `ChunkSize` 不必相同（每端独立切片）

---

## 8. 部署方式

### 8.1 独立守护进程

```bash
go build -o mcte ./cmd/mcte
./mcte -config config/example.yaml
```

进程行为：
- 监听 25565 (Java) + 19132 (Bedrock)
- 真实玩家 → fallback MC server
- 持有合法 UUID 的隧道客户端 → 转发到客户端协商的 `host:port`

### 8.2 接入 Xray

参见 §9 与 `docs/INBOUND_OUTBOUND.md`。

### 8.3 接入 sing-box

参见 §10 与 `docs/INBOUND_OUTBOUND.md`。

### 8.4 信号处理

进程用 `signal.NotifyContext` 监听 SIGINT/SIGTERM → ctx cancel → engine 优雅关闭。

---

## 9. Xray 集成

### 9.1 引入依赖

Xray fork 的 `go.mod`：

```
require github.com/shuffleman/mcte/pkg/integration/xray v0.0.0
replace  github.com/shuffleman/mcte/pkg/integration/xray => /path/to/mcte/pkg/integration/xray
```

### 9.2 inbound 注册胶水

```go
import (
    mcteX "github.com/shuffleman/mcte/pkg/integration/xray"
    "go.uber.org/zap"
)

type xrayDispatcherAdapter struct{ d routing.Dispatcher }
func (a *xrayDispatcherAdapter) Dispatch(ctx context.Context, conn net.Conn, host string, port uint16) error {
    link, err := a.d.Dispatch(ctx, net.Destination{
        Network: net.Network_TCP,
        Address: net.ParseAddress(host),
        Port:    net.Port(port),
    })
    if err != nil { return err }
    return bridgeConnLink(conn, link)
}

func init() {
    common.Must(common.RegisterConfig((*MCTEInboundConfig)(nil), func(ctx context.Context, c interface{}) (interface{}, error) {
        cfg := c.(*MCTEInboundConfig).toMCTEConfig()
        return mcteX.NewServer(cfg, zap.L(), &xrayDispatcherAdapter{routing.DispatcherFromContext(ctx)})
    }))
}
```

### 9.3 outbound 注册胶水

```go
type XrayOutbound struct {
    client *mcteX.Client
}

func (o *XrayOutbound) Process(ctx context.Context, link *transport.Link, dialer internet.Dialer) error {
    ob := session.OutboundFromContext(ctx)
    return o.client.Process(ctx, link.Reader, link.Writer,
        ob.Target.Address.String(), uint16(ob.Target.Port))
}
```

### 9.4 JSON 配置

```jsonc
{
  "inbounds": [{
    "tag": "in-mcte",
    "protocol": "minecraft",
    "settings": {
      "users": [{"uuid": "550e8400-...", "name": "alice"}],
      "fallback": ["127.0.0.1:25566"],
      "listenTcp": "0.0.0.0:25565",
      "listenUdp": "0.0.0.0:19132"
    }
  }],
  "outbounds": [{
    "tag": "out-mcte",
    "protocol": "minecraft",
    "settings": {
      "server": "mcte.example.com",
      "serverPort": 25565,
      "uuid": "550e8400-...",
      "network": "tcp"
    }
  }]
}
```

---

## 10. sing-box 集成

### 10.1 inbound 注册胶水

```go
inbound.Register[mcteSB.Options](registry, "minecraft",
    func(ctx, router, logger, tag, opts) (adapter.Inbound, error) {
        return mcteSB.New(opts, zap.L(), &sbRouteAdapter{router})
    })
```

### 10.2 outbound 注册胶水

```go
outbound.Register[mcteSB.OutboundOptions](registry, "minecraft",
    func(ctx, router, log, tag, opts) (adapter.Outbound, error) {
        impl, err := mcteSB.NewOutbound(opts, zap.L())
        if err != nil { return nil, err }
        return &mcteSBOutbound{
            Adapter: outbound.NewAdapter("minecraft", tag, []string{"tcp", "udp"}),
            impl:    impl,
        }, nil
    })
```

### 10.3 JSON 配置

```jsonc
{
  "inbounds": [{
    "type": "minecraft",
    "tag": "in-mcte",
    "listen": "0.0.0.0:25565",
    "listen_udp": "0.0.0.0:19132",
    "users": [{"uuid": "550e8400-...", "name": "alice"}],
    "fallback": ["127.0.0.1:25566"]
  }],
  "outbounds": [{
    "type": "minecraft",
    "tag": "out-mcte",
    "server": "mcte.example.com",
    "server_port": 25565,
    "uuid": "550e8400-...",
    "network": "tcp"
  }]
}
```

---

## 11. 客户端 SDK

### 11.1 基本用法

```go
import "github.com/shuffleman/mcte/pkg/client"

c, err := client.New(client.Config{
    Server:  "mcte.example.com",
    Port:    25565,
    UUID:    "550e8400-e29b-41d4-a716-446655440000",
    Network: "tcp",                    // 或 "udp" (Bedrock)
})
if err != nil { ... }

conn, err := c.Dial(ctx, "target.host.com", 8080)
defer conn.Close()
conn.Write([]byte("hello"))
buf := make([]byte, 1024)
n, _ := conn.Read(buf)
```

### 11.2 启用流量画像（推荐）

```go
c, _ := client.New(client.Config{
    Server: "mcte.example.com", Port: 25565,
    UUID: "550e8400-...",
    Mimic: client.DefaultProfile(),    // 启用
})
```

### 11.3 自定义画像

```go
Mimic: &client.MimicProfile{
    ChannelPrefix:     "mcte:",
    MoveSuffix:        "m",
    FlySuffix:         "f",
    ChunkSuffix:       "c",
    IdleSuffix:        "i",
    MoveThreshold:     16,             // ≤16B 模拟移动
    FlyThreshold:      128,            // ≤128B 模拟飞行
    ChunkSize:         4096,           // 大包按 4KB 切
    HeartbeatInterval: 2 * time.Second,
}
```

### 11.4 协议伪装

```go
client.Config{
    PretendHost:    "mc.example.com",  // 伪装 handshake 中的 host
    PretendVersion: 769,               // 伪装协议版本
}
```

---

## 12. 背压与流控

### 12.1 设计哲学

借鉴 sing-box VLESS 风格：**应用层不做写队列**，背压完全由 TCP 内核 buf 决定。对 UDP（Bedrock），由 RakNet cwnd 提供等价机制。

### 12.2 各路径背压源

| 路径 | 写阻塞机制 | 读阻塞机制 |
|---|---|---|
| Java framer | TCP socket buf 满 | TCP 内核缓冲 |
| netutil.Pipe | TCP buf 满 → io.Copy 阻塞 → 反压链 | 同 |
| Java bridge | TCP buf 满 + WatchdogConn 30s 硬上限 | 同 |
| Bedrock cwnd | `ResendQueue.WaitForRoom(ctx)` 阻塞至 cwnd 有空间 | recvCh 满 → dispatchLoop 阻塞 → inbox 满 → Feed 丢 datagram → 远端 RTO + cwnd 减半 |

### 12.3 RakNet 拥塞控制

```
初始：cwnd=32, ssthresh=256

每收到一个新 ACK：
  if cwnd < ssthresh:
    cwnd += 1                    // slow start
  else:
    每 cwnd 个 ACK cwnd += 1     // congestion avoidance

丢包（NAK 或 RTO）：
  ssthresh = max(cwnd/2, minCwnd=4)
  cwnd = ssthresh

上限：maxCwnd = 2048
```

### 12.4 WatchdogConn

针对 TCP 半开连接 / 慢消费者，独立 goroutine 监测 Write 阻塞时长：

```
inflight 计数器：
  - 进入 Write：cnt 从 0→1 时 startedAt = now
  - 退出 Write：cnt 从 1→0 时 startedAt = 0

后台 goroutine 周期检查：
  if startedAt != 0 && now - startedAt > timeout:
    conn.Close()
```

**为何不用 SetWriteDeadline**：deadline 是 per-conn 全局状态，多 writer 共享时任一 Write 都会重置 deadline，导致真正卡死的 Write 永远不被检测。

---

## 13. 断开事件传递

任一端断开都会传递到另一端。

### 13.1 Java passthrough

```
client/backend 任一断开
  → io.Copy(EOF/err)
  → netutil.Pipe.closeBoth()
  → 另一端 Close → 反方向 io.Copy 退出
  + Watchdog 30s 兜底
```

### 13.2 Java tunnel

```
任一方向 err / ctx 取消
  → errCh / ctx.Done
  → bridge.closeBoth(client, upstream)   ← 同步关两端
  → 另一 goroutine 解阻塞退出
  + KeepAlive ctx 取消退出
  + Watchdog 30s 兜底
```

### 13.3 Bedrock passthrough / tunnel

```
任一 sess.Closed channel 触发：
  · DisconnectNotification 收到
  · idle 30s
  · ctx 取消
→ ReadApp / WriteAppCtx 监听 closed channel 立即返回
→ defer 关另一端
```

---

## 14. 防爆设计

| 攻击向量 | 防护 |
|---|---|
| FragmentAssembler 内存放大 | LRU(64) + TTL(5s) + total 阈值(4096) + 累积字节(16MB) |
| UDPListener.sessions 爆炸 | maxSessions = max_concurrent；半开 session short idle (10s) |
| ratelimit IP map 爆炸 | LRU(16384) + GC(30s) |
| Engine accept 爆炸 | sem chan，满直接 Close |
| RakNet AckCollector | 4096 阈值，满立即 flush |
| RakNet OrderingBuffer | 1024 阈值，超出强制跳过 expected |
| Session.recvCh 满 | deliver 阻塞 → dispatchLoop 阻塞 → inbox 满丢 datagram → 远端重传反压 |

### 14.1 单 Bedrock session 内存上限

| 项 | 上限 |
|---|---|
| inbox (datagram pointer × 512) | ~4KB |
| recvCh (256 packet pointer) | ~2KB |
| ordering buffer × 16 channel × 1024 entry | 视实际乱序而定 |
| FragmentAssembler (64 group × 4MB) | 256MB（攻击极限，正常 <1MB） |
| ResendQueue (cwnd × MTU = 2048 × 1492) | ~3MB |
| 5 个 goroutine 栈 | ~10KB |
| **典型稳态** | **100-500KB** |
| **攻击极限** | **~4MB** |

---

## 15. 安全特性

### 15.1 抗主动探测

| 探测方式 | 结果 |
|---|---|
| 端口扫描（无 UUID） | 完整 MC 服务器响应（fallback 透传） |
| 错误 UUID | 静默透传，与未知客户端等价 |
| Mojang 正版认证扫描 | MCTE 不参与；fallback 行为决定 |
| 流量包大小画像 | **mimic 启用**时：移动/飞行/区块/心跳分布，与真实玩家相符 |
| TLS fingerprint | 不使用 TLS，无指纹 |
| HTTP 路径探测 | 不监听 HTTP，N/A |

### 15.2 认证算法

- 单 round-trip：客户端发 UUID，服务端 lookup
- 无 PSK 概念（VLESS 风格）
- 多用户分离
- UUID 错误降级真 MC server

### 15.3 资源限制

- accept 总并发：`session.max_concurrent` (10000)
- session 数：UDP listener `maxSessions`
- 半开 session 短超时：10s
- 后台 goroutine 全部 ctx-aware
- 所有 unbounded map 都有 LRU + TTL

---

## 16. 测试覆盖

```
go build ./...            exit 0   (主仓 + xray + singbox)
go vet ./...              exit 0
go test ./...             ok 52/52 PASS
```

| 测试文件 | 用例数 | 覆盖 |
|---|---|---|
| token_test.go | 5 | UUID 提取 / Validator |
| varint_test.go | 3 | VarInt 边界 |
| framer_test.go | 3 | Roundtrip / Truncated / TooLarge |
| framer_concurrent_test.go | 1 | 32×50 包并发写 1600 包全唯一 |
| restored_conn_test.go | 1 | TeeReader+原 conn 完整重读 |
| detector_java_test.go | 4 | Vanilla / Tunnel / Unknown UUID 降级 / Truncated |
| raknet_test.go | 5 | 地址编解码 / Offline Ping / OCR1 Reply / Encapsulated / ACK |
| bedrock_test.go | 4 | VarUint / 批包 / Snappy / PlayStatus |
| backpressure_test.go | 5 | Resend cwnd + AckCollector + OrderingBuffer |
| antiflood_test.go | 5 | Fragment LRU/TTL/MaxTotal + Ratelimit LRU |
| bedrock_backpressure_test.go | 1 | deliver 阻塞 + idle 自动 close |
| watchdog_test.go | 3 | 单卡 / 卡死+正常混合 / 不误关 |
| close_propagation_test.go | 2 | Pipe+Watchdog close 双向传递 |
| reverse_backpressure_test.go | 2 | TimeoutConn 写超时 |
| **mimic_test.go** | **6** | **Mimic Pack / 切片 / Roundtrip / 心跳 / 前缀匹配 / 空输入** |
| **mimic_end_to_end_test.go** | **1** | **Mimic inbound + outbound 全链路（8B/100B/4000B 三种 size）** |
| end_to_end_test.go | 1 | Java inbound + outbound 全链路（非 mimic 模式） |

### 关键测试详解

**TestMimicEndToEnd**（新增）— 验证流量画像端到端：
1. 启动 echo TCP server
2. MCTE inbound 配置 `tunnel.mimic.enabled=true`
3. client SDK 配置 `Mimic: DefaultProfile()`
4. 分别发送 8B / 100B / 4000B 数据
5. 校验 echo 返回字节完整且顺序正确 ✓

**TestMimicChannelByMessageSize** — 验证 size → channel 映射：
- 1B / 32B → mcte:m
- 33B / 256B → mcte:f
- 257B / 4000B → mcte:c

**TestMimicChunkSplit** — 验证大包分片：3500B 按 1000B 切 → 4 个 mcte:c 包，最后一片 500B

**TestMimicRoundtrip** — 5000B Pack 切多片，拼回字节完全相等

**TestJavaEndToEnd** — 验证非 mimic 兼容模式（单 channel）

**TestFramerConcurrentWriteNoInterleave** — 验证 framer 无锁安全：32 goroutine × 50 包，全唯一无交错

**TestWatchdogStuckAmidActiveTraffic** — 验证 watchdog 关键场景：一个 goroutine 卡死 + 另一个高频写仍能检出卡死

---

## 17. 关键约束

1. **goroutine 生命周期**：所有 goroutine 必须能被 ctx 取消。
2. **包 ID 不硬编码**：必须通过 `versions.PacketID(proto, state, dir, name)` 查询。
3. **透传不解析**：passthrough 路径 RestoredConn 已读字节由 io.Copy 自然回放，不重新序列化。
4. **RestoredConn**：buffer 耗尽后无缝切换到原 conn，不得提前返回 io.EOF。
5. **Plugin Message 分帧**：单帧 data 字段最大 32767 字节。
6. **ChunkData 格式**：1.18+ Paletted Container（项目限定 1.21.4+，自然落在 1.18 之后）。
7. **integration 子模块**：独立 go.mod，互不依赖。
8. **抗探测降级**：UUID 缺失 / 无效 / 不在列表 → 静默透传到 fallback，不报错。
9. **KeepAlive 独立**：与 bridge 双向 io 在独立 goroutine。
10. **WatchdogConn 优先**：多 writer 共享 conn 时必须用 WatchdogConn，不用 TimeoutConn。
11. **Mimic 一致性**：客户端 MimicProfile 与 inbound MimicMatcher 的 prefix/suffix 必须一致；阈值不强求但下行画像对称推荐相同。
12. **Mimic 字节顺序**：Pack 严格按输入顺序切片；inbound 顺序写入 upstream → 应用层数据无序变化或截断。

---

## 18. 已知限制

1. **Bedrock 客户端透传依赖非加密通道**：若后端要求 Bedrock 加密握手（ServerToClientHandshake），需要额外实现。
2. **integration/xray + integration/singbox 提供库 + 胶水说明**：实际注册需要用户在 Xray / sing-box fork 中写约 50 行胶水代码。
3. **未跑 10000 并发压测**：建议在真实流量环境验证 `runtime.NumGoroutine` 与 RSS。
4. **Java EncryptionRequest 未启用**：MCTE 用 UUID 认证替代 Mojang 验证，Login 阶段直接发 LoginSuccess。
5. **Bedrock 离线握手 MOTD 固定**：当前 listener 用一个静态 MOTD 回 Unconnected Pong，可改为从配置注入。
6. **同 UUID 多用户并发**：不限制单 UUID 并发数，由 `session.max_concurrent` 总量限制；如需 per-user 限制可加 ratelimit plugin。
7. **Mimic 仅 Java 路径**：Bedrock 由于已有自己的 packet ID 体系（PacketTunnelData = 0x200），未实现 Mimic 多 channel 切换。如需对 Bedrock 同样做画像，可在 PacketTunnelData 之外引入额外 packet ID。
8. **Mimic 时序无抖动**：当前按真实数据大小直接切，未在大小或时间维度引入随机抖动。`tunnel/bridge.go` 内预留了 `jitter()` API，未来可扩展。

---

## 附录：文件统计

| 模块 | Go 文件数 |
|---|---|
| internal/netutil | 3 |
| pkg/auth | 1 |
| pkg/client | **5** (新增 mimic.go) |
| pkg/config | 2 |
| pkg/detector | 5 |
| pkg/engine | 1 |
| pkg/listener | 3 |
| pkg/memory | 2 |
| pkg/passthrough | 4 |
| pkg/plugin (含内置) | 5 |
| pkg/protocol/java | 10 |
| pkg/protocol/bedrock (含 raknet) | 12 |
| pkg/scheduler | 3 |
| pkg/session | 3 |
| pkg/tunnel | 6 |
| pkg/integration/xray | 5 |
| pkg/integration/singbox | 5 |
| cmd/mcte | 1 |
| tests | **17 文件 (52 用例)** (新增 mimic_test.go + mimic_end_to_end_test.go) |
| **合计** | **93 Go 文件** |

非 Go 文件：3 个 go.mod、1 个 example.yaml、4 个 .md。

---

**End of DELIVERABLES.md**
