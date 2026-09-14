# MCP 数据收发服务

## 为只能以插件方式使用AI\或只能用该公司自己的带AI的IDE时，建立一个本地通道以API的方式调用AI，实现灵活用AI（如代码调用）

本地运行的"程序 ⇄ AI"数据桥梁：你的程序把文本/图片推给服务，AI（如 CodeBuddy）通过 MCP 读取并返回结果，程序再取回 AI 的回答。

- **仅依赖 Python 标准库**（Python 3.7+，无需 `pip install` 任何东西）
- 纯本地运行（默认只监听 `127.0.0.1`，数据不出本机）
- 图片自动保存到 `received_images/`，消息持久化到 `messages.jsonl`

## 一、运行前你需要自己准备的东西

| 项目 | 说明 |
|---|---|
| Python 3.7 或更高 | 官网下载安装即可，无需任何第三方库 |
| 端口 8765（默认） | 如被占用可改配置或换端口，见下文 |
| CodeBuddy 或其他 MCP 客户端 | 可选，用于 AI 侧读取数据 |

**不需要**：安装依赖包、注册账号、申请任何 API 密钥——服务的 `api_key` 首次运行时**自动生成**。

## 二、快速启动

1. **启动服务**：双击 `run.bat`（或命令行执行 `python mcp_service.py`）。
   首次运行会自动生成 `config.json`，其中包含随机生成的 `api_key`，并在启动窗口直接显示：

   ```
   接收接口(其他程序推送数据):
     POST http://127.0.0.1:8765/receive
     请求头: X-API-Key: dk_xxxxxxxxxxxxxxxx...
   ```

2. **记下窗口里的 `api_key`**（也可以打开 `config.json` 查看）。这就是你程序推送数据时要用的密钥。

3. **（可选）连接 CodeBuddy**，见第三节。

4. **推送数据测试**：双击 `run_ui.bat` 打开图形测试工具，填入 base_url 和 api_key 即可发文字/图片；或参考 `examples/` 里的例程自己写客户端。

## 三、配置

### config.json（首次运行自动生成，可手动修改）

```json
{
  "api_key": "dk_xxxx...",      // 推送数据的密钥，可自己改成任意字符串
  "http_host": "127.0.0.1",     // 监听地址，默认仅本机
  "http_port": 8765             // HTTP 端口
}
```

- 换端口：改 `http_port` 后重启；或临时用 `python mcp_service.py --port 9000` 覆盖。
- 重置密钥：删除 `config.json` 后重启，会自动生成新 key。

### 连接 以CodeBuddy示例（二选一）

**方式 A：stdio（推荐）** —— 在 CodeBuddy 的 MCP 设置中添加：

```json
{
  "mcpServers": {
    "gm-data-bridge": {
      "transport": "stdio",
      "command": "python",
      "args": ["<你的项目路径>/mcp_service.py", "--stdio"],
      "disabled": false,
      "description": "数据收发桥：其他程序通过 base_url+api_key 推送文本/图片，AI 通过工具读取"
    }
  }
}
```

stdio 模式下服务会随 CodeBuddy 自动启动，并同时开启 HTTP 接收端口。

**方式 B：Streamable HTTP** —— 服务先以 `run.bat` 启动，MCP 连接地址填 `http://127.0.0.1:8765/mcp`，请求头带 `X-API-Key: <你的 api_key>`。

### AI 侧怎么读数据

对 AI 说一句即可，例如：

> 循环调用 get_new_data（wait_seconds=15），循环100次

或：**"看看有没有新数据"**（AI 收到新数据时会有 `收到新数据 #N` 的通知提示）。

## 四、HTTP 接口速查（所有请求带请求头 `X-API-Key`）

| 接口 | 说明 |
|---|---|
| `POST /receive` | 推送数据：`{"text": "..."}` / `{"images": [{"data": "<base64>", "mime": "image/png"}]}` / 两者同发 |
| `GET /responses` | 接收 AI 返回结果：`?after_id=N&wait_seconds=0-60`（游标 + 长轮询） |
| `GET /data` | 查询全部收到的数据（`?limit=20`） |
| `GET /health` | 健康检查 |

**客户端使用要点**（`examples/` 例程已封装好）：

- **游标机制**：程序里维护 `after_id`（上次读到的最大编号），每次 `receive_results` 后更新，不重复不漏。
- **长轮询**：`wait_seconds=10` 表示没新结果时服务端等 10 秒再返回，外部程序可放心用到 60 秒。
- **AI 侧配合**：AI 收到数据后必须调用 `send_result`，结果才会出现在 `/responses` 里。

## 五、MCP 工具列表（AI 侧可用）

| 工具 | 功能 |
|---|---|
| `get_new_data` | 读取未读新数据（支持 `wait_seconds` 长轮询，建议 ≤15 秒） |
| `get_all_data` | 读取全部数据 |
| `clear_data` | 清空数据 |
| `send_result` | AI 返回处理结果给外部程序 |

## 六、例程与测试工具

| 文件 | 说明 |
|---|---|
| `examples/client_example.py` | Python 例程（仅标准库，3.7+） |
| `examples/ClientExample.java` | Java 例程（仅 JDK 11+ 标准库，无第三方依赖） |

两个例程都封装了 4 个方法，可直接复制到你的项目里用：
`send_text(text)`、`send_image(path)`、`send_text_image(text, path)`、`receive_results(after_id, wait_seconds)`。

> 使用例程前，把代码里的 `API_KEY` 占位符改成你自己的 key（见 `config.json` 或启动窗口输出）。

附带的测试工具：
双击`run_ui.bat` 启动 `ui_tester.py`：图形界面测试工具，可视化发送文字/图片、查看 AI 返回。

## 七、目录文件说明

```
mcp_service.py          主程序（单文件，仅标准库）
run.bat                 启动服务（HTTP 模式）
run_ui.bat              启动图形测试工具
ui_tester.py            图形测试工具源码
config.json             配置（首次运行自动生成，已加入 .gitignore）
codebuddy_mcp_config.json  CodeBuddy stdio 配置示例
examples/               Python / Java 客户端例程
messages.jsonl          收到的消息持久化（运行时生成）
responses.jsonl         AI 返回结果持久化（运行时生成）
read_state.json         已读状态（运行时生成）
received_images/        收到的图片（运行时生成）
```

## 八、隐私与安全说明

- 服务默认只监听 `127.0.0.1`，外部设备无法访问。
- `config.json`（含 api_key）及所有运行时数据文件均已列入 `.gitignore`，不会被提交到 Git。
- 例程中的 `API_KEY` 均为占位符，需替换为你本机 `config.json` 中的实际值。
