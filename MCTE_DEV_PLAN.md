# MCTE — Minecraft Transport Engine
## 开发规划文档

---

## 项目定位

MCTE 是一个基于 Minecraft 协议的流量伪装传输网关，使用 Go 实现，作为独立 Transport Engine 存在，可对接 Xray-core 或 sing-box 作为其 inbound transport。

对外表现为标准 Minecraft 服务器，监听 Java 25565（TCP）和 Bedrock 19132（UDP）。收到连接后根据协议指纹判断客户端类型，分两条路径处理：真实 MC 客户端透明转发到后端真实 MC 服务器；隧道客户端剥离 MC 外壳后承载上层任意 TCP 流量。

抗检测原则：任何未携带合法 token 的连接，网关行为与真实 MC 服务器完全一致，不暴露任何额外特征。

---

## 客户端识别机制

识别发生在 Handshake 阶段，不依赖任何外部服务，本地毫秒内完成判断。

Java 侧利用 Handshake 包的 serverAddress 字段。该字段标准值为纯域名或 IP，BungeeCord 等主流代理已有在此字段附加 `\x00` 分隔数据的惯例，因此附加内容不会触发任何异常检测。隧道客户端在 serverAddress 后附加 `\x00token`，token 由预共享密钥（PSK）做 HMAC-SHA256 生成，包含时间戳（±30 秒窗口防重放）。网关读取该字段，若无附加数据或 HMAC 验证失败，判定为真实 MC 客户端。

Bedrock 侧利用 LoginPacket 中 clientData JWT 的自定义扩展字段携带 token，验证逻辑与 Java 侧相同。

不采用 Mojang 正版认证作为区分手段，原因是：认证需请求 Mojang 外部服务，在目标部署环境（需翻墙的网络）下不可靠；每次连接在 Mojang 留有日志；隧道客户端需持有正版账号；认证引入不可控延迟。HMAC token 方案在抗主动探测效果上与正版认证等价，且无上述副作用。

---

## 技术选型

| 项目 | 选择 |
|------|------|
| 语言 | Go 1.22+ |
| Java MC 协议 | 手写实现，覆盖协议版本 766（1.20.x）和 769（1.21.x） |
| Bedrock 协议 | 手写 RakNet 可靠性层 + Bedrock 包层 |
| 加密 | 标准库 crypto/rsa、crypto/aes，HMAC-SHA256 |
| 压缩 | 标准库 compress/zlib，Bedrock 批包用 snappy |
| 配置 | YAML，mapstructure 解析 |
| 日志 | zap |
| 指标 | prometheus/client_golang |
| 测试 | 标准库 testing + testify |

---

## 目录结构

```
mcte/
├── cmd/
│   └── mcte/
│       └── main.go
│
├── pkg/
│   ├── config/
│   │   ├── config.go
│   │   └── loader.go
│   │
│   ├── engine/
│   │   └── engine.go
│   │
│   ├── listener/
│   │   ├── tcp.go
│   │   ├── udp.go
│   │   └── hybrid.go
│   │
│   ├── detector/
│   │   ├── detector.go
│   │   ├── java.go
│   │   ├── bedrock.go
│   │   ├── token.go
│   │   └── restored_conn.go
│   │
│   ├── passthrough/
│   │   ├── handler.go
│   │   └── dialer.go
│   │
│   ├── tunnel/
│   │   ├── handler.go
│   │   ├── negotiate.go
│   │   ├── virtual_play.go
│   │   ├── keepalive.go
│   │   └── bridge.go
│   │
│   ├── protocol/
│   │   ├── java/
│   │   │   ├── packet.go
│   │   │   ├── varint.go
│   │   │   ├── framer.go
│   │   │   ├── crypto.go
│   │   │   ├── compression.go
│   │   │   ├── handshake.go
│   │   │   ├── login.go
│   │   │   ├── configuration.go
│   │   │   ├── play.go
│   │   │   └── versions.go
│   │   │
│   │   └── bedrock/
│   │       ├── raknet/
│   │       │   ├── session.go
│   │       │   ├── reliability.go
│   │       │   ├── ack.go
│   │       │   ├── fragment.go
│   │       │   └── window.go
│   │       ├── login.go
│   │       ├── packet.go
│   │       └── batch.go
│   │
│   ├── session/
│   │   ├── session.go
│   │   ├── manager.go
│   │   └── pool.go
│   │
│   ├── scheduler/
│   │   ├── tick.go
│   │   ├── packet_scheduler.go
│   │   └── shaper.go
│   │
│   ├── memory/
│   │   ├── pool.go
│   │   └── ring.go
│   │
│   ├── plugin/
│   │   ├── interface.go
│   │   ├── registry.go
│   │   ├── hooks.go
│   │   └── builtin/
│   │       ├── metrics/plugin.go
│   │       └── ratelimit/plugin.go
│   │
│   └── integration/
│       ├── xray/
│       │   ├── register.go
│       │   ├── listener.go
│       │   ├── conn.go
│       │   └── config.go
│       └── singbox/
│           ├── register.go
│           ├── inbound.go
│           ├── conn.go
│           └── options.go
│
├── internal/
│   └── netutil/
│       ├── timeout_conn.go
│       └── pipe.go
│
├── config/
│   └── example.yaml
│
├── tests/
│   ├── detector_test.go
│   ├── passthrough_test.go
│   ├── tunnel_test.go
│   └── bench/
│       └── session_bench_test.go
│
├── go.mod
└── README.md
```

---

## 模块说明

### config

config.go 定义全局配置结构体，覆盖监听地址、PSK、后端目标、session 上限、日志级别、指标开关等所有可配置项。loader.go 负责读取 YAML 文件并做合法性校验，非法配置启动时直接 fatal。

### engine

Engine 是整个系统的根对象，负责按配置组装所有模块、管理启动和停止顺序。核心分发逻辑在此：从 listener 接收 conn，交给 detector 判断类型，再路由到 passthrough handler 或 tunnel handler。每个 conn 在独立 goroutine 中处理，goroutine 生命周期由 context 控制。

### listener

tcp.go 封装标准 net.Listener，维护 accept loop，将 accepted conn 送入 channel。udp.go 封装 net.PacketConn，处理 RakNet 的无连接握手，模拟连接语义后同样送入 channel。hybrid.go 同时启动 TCP 和 UDP 两个 listener，对外提供统一的 conn channel，engine 无需关心底层协议类型。

### memory

pool.go 实现分级字节池，内部用 8 个 sync.Pool，分别对应 64B 到 32KB 的 2 的幂次大小。调用方按需取用，用完归还，超出最大级别的直接 make 不入池。ring.go 实现无锁单生产者单消费者环形缓冲，专用于 RakNet UDP 高频收包路径，避免加锁开销。

### protocol/java

varint.go 实现 VarInt 和 VarLong 的读写，同时提供操作 []byte 的零拷贝版本。framer.go 负责读取以 VarInt 为长度前缀的完整包，支持可插拔的加密和压缩 middleware。crypto.go 封装 AES-CFB8 加解密 stream，在 Login 阶段协商完成后包裹在 framer 的 reader/writer 上。compression.go 封装 zlib，在 SetCompression 包之后启用。

handshake.go、login.go、configuration.go、play.go 分别对应 MC 协议的四个状态，只实现本项目所需的包，不需要实现完整的游戏协议。versions.go 维护协议版本号到包 ID 的映射表，所有包 ID 必须通过此表查询，不允许硬编码。

需要实现的具体包如下：

handshake 状态：Handshake 包（客户端发，含 protocolVersion、serverAddress、serverPort、nextState）。

login 状态：LoginStart（客户端发）、EncryptionRequest（服务端发）、EncryptionResponse（客户端发）、SetCompression（服务端发）、LoginSuccess（服务端发）、LoginPluginRequest（服务端发）、LoginPluginResponse（客户端发）。

configuration 状态：FinishConfiguration（双向）、PluginMessage（双向）。

play 状态（仅 Virtual Session 所需最小集）：KeepAlive 客户端方向和服务端方向、ChunkDataAndUpdateLight、GameEvent、SynchronizePlayerPosition、PluginMessage 客户端方向和服务端方向。

### protocol/bedrock/raknet

按 ack.go → fragment.go → reliability.go → window.go → session.go 的顺序实现。session.go 整合其余四个模块，驱动状态机从 UNCONNECTED 到 CONNECTED，对上层提供类似 net.Conn 的读写接口。

### detector

token.go 实现 HMAC-SHA256 token 的生成与验证。生成时将当前时间戳（Unix 秒，8 字节大端）与 PSK 做 HMAC，结果 base64url 编码后与时间戳拼接输出。验证时先解码，检查时间戳是否在 ±30 秒窗口内，再验证 HMAC。

restored_conn.go 实现 RestoredConn，将 TeeReader 已读取的字节（存于 bytes.Buffer）和原始 conn 通过 io.MultiReader 拼合，对外表现为完整的 net.Conn。Read 方法先消耗 buffer，buffer 耗尽后无缝切换到原始 conn，不得提前返回 io.EOF。

java.go 用 TeeReader 包裹 conn 读取 Handshake 包，解析 serverAddress，验证 token，返回 ClientKind 和 RestoredConn。bedrock.go 同理，读到 LoginPacket 后解析 JWT clientData 中的 token 字段。detector.go 是统一入口，根据连接类型自动选择 Java 或 Bedrock 路径。

Detector 的不变契约：不消耗超过第一个完整 Handshake 包的字节；返回的 conn 必须是 RestoredConn，调用方可完整重读所有已读字节；发生 IO 错误时关闭 conn 并返回 error，不泄漏 goroutine。

### passthrough

dialer.go 维护后端 MC 服务器地址列表，支持多个地址轮询，连接超时可配置。handler.go 接收 RestoredConn 和原始握手字节，连接后端后先将握手字节原样写入后端（不重新序列化，直接 Write 原始字节），然后调用 netutil.Pipe 做双向透传，任一端断开立即关闭另一端。透传路径不对任何流量做解析。

### tunnel

negotiate.go 在 Login 阶段完成后，服务端向客户端发 Login Plugin Request，channel 名为自定义标识符，等待客户端回 Login Plugin Response，从 response 的 data 字段解析目标地址和端口。data 字段格式：1 字节版本号 + 2 字节地址长度（大端序）+ 地址 UTF-8 字节 + 2 字节端口（大端序）。

virtual_play.go 发送让客户端进入 Play 状态且不报错的最小包序列，依次为：LoginSuccess、FinishConfiguration、Play 阶段 Login 包（GameMode=Spectator）、SynchronizePlayerPosition（坐标 0,64,0）、ChunkDataAndUpdateLight（(0,0) 全空气 chunk）、GameEvent（世界已加载通知）。包 ID 必须查 versions.go，不同版本不同。1.18 前后 ChunkData 格式完全不同（1.18 引入 Paletted Container），必须按版本号分支处理，不允许用同一格式覆盖所有版本。

keepalive.go 维护 KeepAlive 定时器，Java 每 20 秒发送一次，等待客户端响应，超时 30 秒断开。KeepAlive 必须在独立 goroutine 中运行，不能被 bridge 的读写操作阻塞。

bridge.go 双向桥接 MC Plugin Message 流与上层 net.Conn。从客户端读 channel 为配置指定值（默认 mcte:data）的 Plugin Message，提取 data 字段写入上游；从上游读数据，按最大 32767 字节分帧封装为 Plugin Message 写给客户端。KeepAlive 与 bridge 并行运行，使用独立 goroutine 加 select，不串行。context 取消时立即返回并关闭两端连接。

handler.go 是隧道模式入口，按顺序调用：完成 MC Login 握手（不做正版验证）→ negotiate → virtual_play → 连接上游 → bridge。

### session

session.go 定义 Session 结构体，包含 ID、ClientKind、ProtocolType、版本号、FSM 状态、RemoteAddr、创建时间、最后活跃时间（atomic int64）。manager.go 用 16 个 shard（分片 sync.Map，按 ID 取模路由），支持按 ConnID 和 RemoteIP 查询。独立 goroutine 每 30 秒扫描超时 Session 并关闭。pool.go 用 sync.Pool 复用 Session 结构体，归还时清零所有字段。

### scheduler

tick.go 实现 20TPS Ticker，使用 time.NewTicker 而非 time.Sleep，通过滑动窗口补偿 goroutine 调度抖动导致的 tick 漂移。packet_scheduler.go 维护优先级队列（KeepAlive > 游戏状态包 > Chunk > Entity），每 tick 按 sendBudget 限额 drain 发送。shaper.go 封装 golang.org/x/time/rate 的 Limiter，实现 token bucket，控制单连接和全局带宽上限。

### plugin

interface.go 定义三个接口：Plugin（生命周期管理）、PacketHook（入站/出站包拦截，可修改或丢弃）、SessionHook（连接打开/关闭/状态变更回调）。registry.go 维护已注册插件列表，按顺序调用 hook。内置插件：metrics 暴露 Prometheus 指标（活跃连接数、透传与隧道分别计数、带宽用量）；ratelimit 按源 IP 限制连接速率。

### integration/xray

MCTE 注册为 Xray 的一种自定义 Transport，通过 init() 调用 internet.RegisterTransportListener 和 internet.RegisterTransportDialer 注册，主程序 import 此包即激活。listener.go 实现 internet.Listener，内部启动 Engine，每个 Session 封装为 XrayConnAdapter 交给 Xray 的 ConnHandler。conn.go 实现 net.Conn，Read/Write 对接 tunnel bridge 的上游端，对 Xray 完全透明。config.go 定义 MCTEConfig 结构体，对应 JSON 配置中的 mcteSettings 字段。

Xray JSON 配置中 inbound 的 streamSettings.network 设为 "minecraft"，mcteSettings 包含 mode（forward/virtual/hybrid）、javaEnabled、bedrockEnabled、maxSessions 等字段。

### integration/singbox

MCTE 注册为 sing-box 的一种 inbound 类型，通过 init() 调用 adapter.RegisterInbound 注册，类型名为 "minecraft"。inbound.go 实现 adapter.Inbound 接口，Start 时启动 Engine，每个 Session 通过 router.RouteConnection 交给路由层，metadata.Destination 从隧道协商的 TunnelMeta 中取得。conn.go 适配 sing 的 Conn 接口。options.go 定义 JSON 配置结构体。

两个 integration 包各自独立 go.mod，互不依赖，按需 import，避免 Xray 和 sing-box 的依赖树互相污染。

### internal/netutil

timeout_conn.go 封装带 deadline 的 conn，对超时行为做统一处理。pipe.go 实现双向 io.Copy，任一方向出错或 context 取消时同时关闭两端，不泄漏 goroutine。

---

## 数据流

### 真实 MC 客户端路径

连接进入 → HybridListener 接收 → Detector 用 TeeReader 读 Handshake，serverAddress 无合法 token → 判定为 MC 客户端，返回 RestoredConn（含原始握手字节）→ PassthroughHandler 连接后端，将原始握手字节写入后端，双向 io.Copy → 连接关闭。

### 隧道客户端路径

连接进入 → HybridListener 接收 → Detector 读 Handshake，token HMAC 验证通过 → 判定为隧道客户端，返回 RestoredConn → TunnelHandler 完成 MC Login 握手（跳过正版验证）→ negotiate 获取目标地址 → virtual_play 发送最小 Play 状态包 → 连接上游（Xray/sing-box）→ bridge 双向桥接 Plugin Message 与上游，KeepAlive 并行驱动 → 连接关闭。

---

## 配置文件结构

配置文件为 YAML 格式，包含以下顶层字段：

listen 块：tcp 字段指定 Java 监听地址（默认 0.0.0.0:25565），udp 字段指定 Bedrock 监听地址（默认 0.0.0.0:19132）。

tunnel 块：psk 字段为预共享密钥字符串；time_window 字段为 token 有效时间窗口（默认 30s）；channel 字段为 Plugin Message channel 名（默认 mcte:data）。

passthrough 块：targets 字段为后端 MC 服务器地址列表，支持多个；dial_timeout 字段为连接超时（默认 5s）。

session 块：max_concurrent 字段为最大并发 Session 数；idle_timeout 字段为空闲超时时间。

scheduler 块：tick_rate 字段为 TPS（默认 20）；send_budget_per_tick 字段为每 tick 每 Session 最大发送字节数。

log 块：level 字段（debug/info/warn/error）；format 字段（json/console）。

metrics 块：enabled 字段开关；listen 字段为 Prometheus 指标监听地址。

---

## 开发阶段划分

### 阶段一：基础设施

实现 memory/pool.go、memory/ring.go、protocol/java/varint.go、internal/netutil/pipe.go。这四个模块无外部依赖，是其他所有模块的基础，优先完成，后续不再改动。

### 阶段二：协议解析层

实现 protocol/java 下的全部文件，以及 protocol/bedrock/raknet 下的全部文件和 bedrock/login.go。目标是能完整读写 Java Handshake/Login 包，能解析 Bedrock LoginPacket。每个文件完成后立即写单元测试。

### 阶段三：识别与分流

实现 detector 下的全部文件，顺序为 token.go → restored_conn.go → java.go → bedrock.go → detector.go。完成后可对任意连接做出判断并返回可完整重读的 conn。

### 阶段四：核心 Handler

先实现 passthrough 下的全部文件（较简单），再实现 tunnel 下的文件，顺序为 negotiate.go → virtual_play.go → keepalive.go → bridge.go → handler.go。

### 阶段五：组装与集成

按 session → scheduler → plugin → engine 顺序实现。engine 完成后整个系统可端到端运行。最后实现 integration/xray 和 integration/singbox，各自独立 go.mod。

---

## 关键约束

所有 goroutine 必须能被 context 取消，不允许存在无法退出的 goroutine。

所有从 memory pool 取出的 []byte 在函数返回前必须归还，使用 defer 确保。

RestoredConn 的 Read 在 buffer 耗尽后必须无缝切换到原始 conn，不得提前返回 io.EOF。

透传路径不对任何流量做解析，握手字节必须用原始字节直接 Write，不允许重新序列化。

KeepAlive 必须与 bridge 的读写操作并行，不允许串行。KeepAlive goroutine 必须独立，bridge 的上游读写阻塞不影响 KeepAlive 发送。

Plugin Message 单帧 data 字段最大 32767 字节，超出必须分帧。

版本相关的包 ID 必须通过 versions.go 的映射表查询，不允许在任何地方硬编码包 ID。

1.18 前后的 ChunkData 格式完全不同，virtual_play.go 必须按版本号分支处理，不允许用同一格式覆盖所有版本。

integration/xray 和 integration/singbox 各自独立 go.mod，不允许互相依赖。

---

## 测试要求

### 单元测试

detector/token：生成合法 token 后验证通过；过期 token 验证失败；HMAC 错误的 token 验证失败；篡改时间戳的 token 验证失败，共 4 个 case。

detector/java：无附加数据的标准 Handshake 判定为 MC 客户端；携带合法 token 的 Handshake 判定为隧道客户端；token 验证失败的 Handshake 判定为 MC 客户端（降级，不报错）；RestoredConn 可完整重读所有已读字节，共 4 个 case。

protocol/java/varint：边界值 0、1、最大正值、负数均正确编解码。

protocol/java/framer：正常包、超大包（超出限制返回错误）、截断包（返回 io.ErrUnexpectedEOF）。

### 集成测试

passthrough：启动 mock MC 服务端，客户端发送标准 Handshake，验证后端收到与客户端发出完全一致的字节序列，不允许有任何差异。

tunnel：完整隧道握手流程，通过 bridge 双向传输随机数据，验证两端数据一致性。

### 压测

10000 并发连接，持续 60 秒，内存增长不超过 2GB，通过 runtime.NumGoroutine 监控无 goroutine 泄漏。

---

## 交付物检查清单

- [ ] go build ./... 无报错
- [ ] go test ./... 全部通过
- [ ] go vet ./... 无警告
- [ ] 标准 MC 客户端（1.21）连接后透传到后端，游戏可正常进行
- [ ] 携带合法 token 的隧道客户端可完成握手并建立 bridge
- [ ] 不携带 token 的连接行为与真实 MC 服务器完全一致
- [ ] config/example.yaml 可直接用于启动，含完整字段注释
- [ ] 10000 并发压测通过，无内存泄漏，无 goroutine 泄漏
