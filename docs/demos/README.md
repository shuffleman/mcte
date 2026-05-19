# MCTE 接入 sing-box / MoreRay-core 配置示例

四种角色组合：

| 文件 | 角色 | 平台 |
|---|---|---|
| `singbox-server.json` | MCTE inbound（落地节点） | sing-box |
| `singbox-client.json` | MCTE outbound（代理客户端） | sing-box |
| `morerray-server.json` | MCTE inbound（落地节点） | MoreRay-core (Xray fork) |
| `morerray-client.json` | MCTE outbound（代理客户端） | MoreRay-core |

## 拓扑

```
              ┌────────────────────────────────────┐
              │  MCTE Inbound (sing-box / MoreRay) │
              │  TCP 25565  ───┐                   │
              │  UDP 19132  ───┤                   │
              │                │  UUID 鉴权         │
              │  fallback_tcp  │  (未认证流量)      │
              │  fallback_udp  │                   │
              └────────────────┼───────────────────┘
                               │
            ┌──────────────────┴──────────────────┐
            │                                     │
   ┌────────▼───────┐                  ┌──────────▼─────────┐
   │ 真实 Java MC   │                  │  真实 Bedrock MC   │
   │ 127.0.0.1:25566│                  │  127.0.0.1:19133   │
   └────────────────┘                  └────────────────────┘

[客户端]
   你的浏览器 / 应用
        │
        ▼
   MCTE Outbound (sing-box / MoreRay)
        │
        ▼
   通过 MC 协议拨号到上面的 Inbound (UUID 鉴权)
        │
        ▼
   远端目标网站 / API
```

## 通用前提

1. 服务端必须配置至少一个真实 MC server 作为 `fallback`（伪装、抗主动探测）
   - Java 端口默认 25566（与服务端的 25565 区分）
   - Bedrock 端口默认 19133（与 19132 区分）
2. 客户端 UUID 必须与服务端 `users[i].uuid` 完全一致（**两边 UUID 不一致 = 静默丢到 fallback**）
3. 推荐启用 sing-box 客户端的 `mixed` 或 `socks` inbound 暴露给本地应用

## 验证连通性

启动两端后：
```bash
# 客户端 socks 走 mcte outbound
curl -x socks5h://127.0.0.1:1080 https://ifconfig.me
```

应当看到服务端节点的出口 IP。同时本地用 MC 客户端连服务端 25565 也能进 fallback Java MC server（看不出代理痕迹）。

## Trouble-shooting

### MoreRay/Xray: `Listen on AnyIP but no Port(s) set in InboundDetour`

MCTE 自己监听 25565/19132，不通过 Xray 的 receiver/transport 框架，所以 inbound
JSON **不需要** 顶层的 `port` / `listen` 字段。MoreRay-core 已在框架层为
`mcte` / `minecraft` 协议跳过 port 校验（commit `47e71862`）。如果你使用更早的
版本仍报这个错，请 `git pull` 最新 main 分支并重新编译。

### sing-box: inbound 类型未注册

确保你的 sing-box fork 已合并 `protocol/minecraft/` + `option/minecraft.go`，
且 `include/registry.go` 调用了 `protocol_minecraft.RegisterInbound(inboundRegistry)`
与 `protocol_minecraft.RegisterOutbound(outboundRegistry)`。

### 客户端拨号无反应 / 直接关闭

1. **UUID 不匹配最常见**：客户端 `uuid` 必须与服务端 `users[i].uuid` 完全一致
   （大小写、破折号都要对）。任何不一致都会被 detector 静默降级为"未知 MC 客户端"，
   走 fallback 透传 → 看起来像超时。
2. 服务端日志开 debug 后会看到：
   - `java tunnel ended` / `bedrock tunnel ended`：隧道正常断开
   - `java passthrough ended`：未通过认证，走了 fallback
3. 服务端必须能拨到客户端协商出的 `host:port`，否则隧道失败但不会重连。

### Bedrock 离线 ping 没回应

确认服务端 `bedrock_enabled` / `bedrockEnabled` 为 `true`，且 `listen_udp` /
`listenUdp` 已设。

### MOTD 暴露 "MCTE" 品牌

老版本默认 MOTD `"MCPE;MCTE;..."`，已修复为通用 `"Dedicated Server"`（commit
`04f6c4e`）。如需自定义，在服务端 inbound 设 `motd` 字段。

### 流量画像（mimic）

mcte 仓库的客户端 SDK 支持画像模式（按数据大小映射到 move/fly/chunk channel）。
当前 sing-box / MoreRay outbound 配置**未暴露** mimic 字段 —— 实际隧道字节仍能
传输，但流量大小分布不模拟玩家行为。如需画像，自己调用 `client.New(Config{Mimic: client.DefaultProfile()})`。

## 配置升级注意

| 升级前 | 升级后 | 兼容性 |
|---|---|---|
| `fallback: [...]` | `fallback_tcp` + `fallback_udp` 分别配置 | 旧 `fallback` 仍可用作兜底 |
| `MOTD: "MCPE;MCTE;..."` | `MOTD: "MCPE;Dedicated Server;..."` | 自动生效，可通过 `motd` 字段覆盖 |
| `ratelimit` 未生效 | accept 路径强制执行 | 默认关闭，需 mcte engine 配置开启 |

