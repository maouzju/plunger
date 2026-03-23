# Plunger 马桶塞

零配置本地弹性代理，为 Claude Code / Codex CLI / Cursor 等客户端的流式 API 调用提供自动断线恢复。

## 它做什么

Plunger（马桶塞🪠）坐在你的 AI 客户端和 Anthropic API 之间。当流式响应中途断开——无论是网络抖动、服务器错误还是卡顿——它会自动携带已积累的上下文重试请求，不丢失进度。

**支持的客户端：**

| 客户端 | 接管方式 | 状态 |
|---|---|---|
| Claude Code | 改写 `~/.claude/settings.json` 中的 `ANTHROPIC_BASE_URL` | 已验证 |
| Codex CLI | 改写 `~/.codex/config.toml` 中的 base URL | 已验证 |
| Cursor | 通过 `/v1/chat/completions` 端点代理 | 未充分测试 |

**工作原理：**

1. 启动时读取 `~/.claude/settings.json` 和 `~/.codex/config.toml`，捕获当前上游 URL
2. 改写配置，将流量路由到本地代理
3. 监控流的断连、卡顿和错误
4. 失败时，将已接收的部分响应作为 assistant prefill 注入并重试
5. 关闭时恢复原始设置（fail-open 设计）

## 特性

- **自动流恢复** — 检测流中断并携带已积累上下文恢复
- **卡顿检测** — 可配置的首字节超时、块间卡顿超时、可见输出超时
- **SSE 心跳** — 周期性心跳帧保持连接存活
- **指数退避** — 带抖动的重试策略（1s 基础，1.5x 系数，30s 上限）
- **Fail-open 看门狗** — 独立进程监控代理健康状态，代理挂了自动恢复设置
- **多客户端支持** — 同时接管 Claude Code 和 Codex CLI 配置，兼容 Cursor 等 OpenAI 兼容客户端
- **Provider 切换支持** — 检测 CC Switch 切换 provider 无需重启
- **桌面 UI** — 基于 tkinter 的控制面板，实时监控和配置
- **极简依赖** — 仅需 `aiohttp`

## 支持的端点

| 端点 | 说明 |
|---|---|
| `POST /v1/messages` | Anthropic Messages API（流式和非流式） |
| `POST /v1/responses` | Responses API |
| `POST /v1/chat/completions` | OpenAI 兼容的 chat completions |
| `GET /health` | 健康检查 |

## 安装

需要 **Python 3.10+**。

```bash
pip install plunger-proxy
```

或从源码安装：

```bash
git clone https://github.com/maouzju/plunger.git
cd plunger
pip install .
```

## 使用

### 快速开始

```bash
python run.py
```

代理默认启动在 `http://127.0.0.1:8462`，自动接管 Claude 和 Codex 设置，退出时恢复。

### 命令行参数

```
-p, --port              监听端口（默认：8462）
-t, --timeout           卡顿超时秒数（默认：60）
-r, --retries           最大重试次数，-1 为无限（默认：-1）
-u, --upstream          手动指定上游 URL
--max-body-mb           最大请求体大小 MiB（默认：32）
--safe-resume-body-mb   恢复/prefill 最大请求体 MiB（默认：19）
--watch-interval        settings.json 轮询间隔秒数（默认：1）
```

### 桌面 UI

```bash
python proxy_ui.py
```

打开控制面板，可查看实时状态、恢复事件历史和调整配置。

## 恢复原理

```
Client ──► Plunger ──► Anthropic API / OpenAI-compatible API
              │
              ├─ 流正常开始
              ├─ 检测到断连（超时/错误/卡顿）
              ├─ 保存已积累的部分文本
              ├─ 将部分文本作为 assistant prefill 注入重试请求
              └─ 流从断点处继续
```

代理追踪部分响应，重试时将已积累文本注入为 assistant message prefill，告诉 API 从上次停止的地方继续，而不是从头开始。

## 配置文件

| 路径 | 用途 |
|---|---|
| `~/.claude/settings.json` | 被接管以重定向到本地代理 |
| `~/.codex/config.toml` | Codex CLI 配置，被接管 |
| `~/.claude/plunger/` | 恢复数据目录 |
| `~/.claude/plunger/events.json` | 恢复事件历史 |
| `~/.claude/plunger/service.log` | 服务日志 |

## License

[MIT](LICENSE)
