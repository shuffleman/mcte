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
