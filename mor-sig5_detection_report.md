# mor-sig5.pcap MCTE 伪装流量检测报告

> 模型: XGBoost | 训练数据: Vanilla + LuckyBlock + Pixelmon (真实MC) vs daili2 (MCTE)
> 测试数据: mor-sig5.pcap | 窗口: 389 | 特征: 56
> 推测协议栈: MoreRay + sing-box 变体

## 1. 分类结果

| 判定 | 窗口数 | 占比 |
|---|---|---|
| **MCTE 伪装流量** | **386** | **99.2%** |
| Real MC | 3 | 0.8% |

| 置信度 | 窗口数 |
|---|---|
| 高置信 MCTE (prob > 0.9) | 147 (37.8%) |
| 中置信 MCTE (prob 0.5–0.9) | 239 (61.4%) |
| 模糊 (0.3–0.5) | 2 (0.5%) |
| 低置信 Real (0.1–0.3) | 1 (0.3%) |
| 高置信 Real (< 0.1) | 0 |

- **平均 MCTE 概率:** 0.8902
- **漏报 3 窗口 (0.8%)** — 其中最低置信窗口 prob=0.345

## 2. 三数据集横向对比

| 指标 | daili3 (纯 MCTE) | mor-sing4 | mor-sig5 |
|---|---|---|---|
| 窗口数 | 300 | 369 | 389 |
| MCTE 检出率 | 99.0% | 100.0% | 99.2% |
| 平均 MCTE 概率 | 0.920 | 0.877 | 0.890 |
| 高置信占比 (>0.9) | 85.7% | 27.6% | 37.8% |
| 中置信占比 (0.5-0.9) | 13.3% | 72.4% | 61.4% |
| 漏报窗口 | 3 | 0 | 3 |

mor-sig5 的伪装水平与 mor-sing4 接近，均优于 daili3。高置信占比从 85.7% 降至 37.8%。

## 3. 判定依据 (SHAP 全局特征影响力)

| # | 特征 | SHAP | 维度 | 方向 |
|---|---|---|---|---|
| 1 | `seq_delta_rate` | 3.63 | TCP 行为 | MCTE |
| 2 | `c2s_small_pkt_frac` | 1.42 | 包大小分布 | MCTE |
| 3 | `c2s_size_mean` | 1.36 | 包大小分布 | Real |
| 4 | `iat_max_ms` | 0.91 | 到达间隔 | Real |
| 5 | `pl_hfb_s2c_top5_ratio` | 0.51 | 载荷熵 | MCTE |
| 6 | `pl_ent_s2c_mean` | 0.32 | 载荷熵 | MCTE |

### 三数据集 SHAP 对比

| 特征 | daili3 | mor-sing4 | mor-sig5 | 趋势 |
|---|---|---|---|---|
| `seq_delta_rate` | 3.51 | 3.65 | 3.63 | 始终最强信号 |
| `c2s_small_pkt_frac` | 1.41 | 1.44 | 1.42 | 稳定 |
| `iat_max_ms` | 1.19 (→MCTE) | 0.82 (→Real) | 0.91 (→Real) | mor-* 系列翻转 |
| `pl_hfb_s2c_top5_ratio` | 0.33 | 0.54 | 0.51 | mor-* 更明显 |

## 4. 窗口级分析

### 4.1 最高置信窗口 (#234, prob=0.982)

| 特征 | 实际值 | SHAP | 方向 |
|---|---|---|---|
| `seq_delta_rate` | 46.5亿 | +3.40 | MCTE |
| `iat_max_ms` | 208.7ms | +1.50 | MCTE |
| `c2s_small_pkt_frac` | 0.0 | +1.20 | MCTE |
| `c2s_size_mean` | 0.0 | -1.43 | Real |
| `burst_count` | 12 | +0.28 | MCTE |

### 4.2 最低置信窗口 (#262, prob=0.345) — 漏报

| 特征 | 实际值 | SHAP | 方向 |
|---|---|---|---|
| `c2s_small_pkt_frac` | 0.0 | **+2.64** | MCTE |
| `iat_max_ms` | 606.8ms | **+2.09** | MCTE |
| `seq_delta_rate` | 7.2亿 | **-2.08** | **Real** |
| `c2s_size_mean` | 0.0 | -1.16 | Real |
| `iat_std_ms` | 231.5ms | +0.55 | MCTE |
| `pl_ent_s2c_mean` | 2.67 | -0.43 | **Real** |

**漏报原因:** 此窗口的 `seq_delta_rate` = 7.2 亿，接近真实 MC (5亿) 水平，且 `pl_ent_s2c_mean` = 2.67 也偏向 Real。`c2s_small_pkt_frac` 和 `iat_max_ms` 虽强烈指向 MCTE 但不足以扭转。这是 MoreRay+sing-box 伪装效果最好的一个窗口——偶然在 TCP 行为上逼近了真实 MC。

## 5. 三个逃逸窗口的共同特征

3 个漏报窗口均表现为:
- `seq_delta_rate` 方向翻转（SEQ 速率偶然接近真实 MC 水平）
- `pl_ent_s2c_mean` 偏低（载荷熵更接近 MC 明文）
- 但 `iat_max_ms` 和 `c2s_small_pkt_frac` 仍然暴露

## 6. 总结

mor-sig5 与 mor-sing4 特征高度一致，说明 MoreRay+sing-box 组合的伪装策略稳定：

- **IAT 优化有效:** `iat_max_ms` SHAP 方向已翻转向 Real，包间隔维度的伪装较为成功
- **TCP SEQ 偶尔接近:** 在流量低谷窗口，SEQ 速率降至接近真实 MC 水平，导致 3 个窗口侥幸逃逸
- **但 99.2% 仍被检出:** `seq_delta_rate` 平均影响力 3.63 仍远超其他特征，代理双连接架构是根本弱点
- **载荷熵不一致:** 部分窗口熵值低（流量少时），部分高（大流量时），表现不稳定
