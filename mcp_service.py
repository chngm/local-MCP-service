#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MCP 数据收发服务（本地运行，仅依赖 Python 标准库）

功能：
1. 接收接口：POST /receive  —— 其他程序携带 api_key 推送文本/图片
2. MCP  接口：POST/GET /mcp —— AI 工具（如 CodeBuddy）通过 MCP Streamable HTTP 连接
3. 发送接口：MCP 工具 get_new_data / get_all_data / clear_data，HTTP GET /data
4. 主动通知：收到新数据时，通过 SSE(GET /mcp) 向已连接的 MCP 客户端推送
   notifications/message 日志通知（stdio 模式下写到 stdout）
"""

import argparse
import base64
import json
import os
import secrets
import sys
import threading
import time
import uuid
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config.json")
IMAGE_DIR = os.path.join(BASE_DIR, "received_images")
DATA_FILE = os.path.join(BASE_DIR, "messages.jsonl")

DEFAULT_CONSUMER = "default"
STDOUT_LOCK = threading.Lock()

CONFIG = {"api_key": "", "http_host": "127.0.0.1", "http_port": 8765}


def log(msg):
    """日志统一输出到 stderr，保证 stdout 只承载 MCP 协议数据。"""
    try:
        sys.stderr.write("[MCP-Service] %s\n" % msg)
        sys.stderr.flush()
    except Exception:
        pass


# ---------------------------------------------------------------- 配置
def ensure_config():
    if not os.path.exists(CONFIG_PATH):
        cfg = {
            "api_key": "dk_" + secrets.token_hex(16),
            "http_host": "127.0.0.1",
            "http_port": 8765,
        }
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
        log("已生成配置文件 config.json")
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------- 数据存储
READ_STATE_FILE = os.path.join(BASE_DIR, "read_state.json")
RESP_DATA_FILE = os.path.join(BASE_DIR, "responses.jsonl")
RESP_READ_FILE = os.path.join(BASE_DIR, "resp_read_state.json")


class DataStore:
    """
    数据存储（支持多实例共享）：
    - 所有实例都往同一个 jsonl 文件追加写入
    - 每次读取前从磁盘增量加载其他实例写入的数据（跨进程可见）
    - 已读状态持久化到 read_state（重启不会重复投递）
    """

    def __init__(self, data_file=DATA_FILE, read_state_file=READ_STATE_FILE, name="data"):
        self.data_file = data_file
        self.read_state_file = read_state_file
        self.name = name
        self.lock = threading.Lock()
        self.cond = threading.Condition(self.lock)
        self.items = []
        self.next_id = 1
        self._file_pos = 0
        self._read_marks = {}  # id(str) -> [consumer, ...]
        self._load()

    # ---- 初始化 ----
    def _load(self):
        self._load_read_marks()
        if os.path.exists(self.data_file):
            try:
                with open(self.data_file, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            it = json.loads(line)
                            it["read_by"] = self._read_marks.get(str(it.get("id")), [])
                            self.items.append(it)
                if self.items:
                    self.next_id = max(i["id"] for i in self.items) + 1
                log("[%s] 已加载历史数据 %d 条" % (self.name, len(self.items)))
            except Exception as e:
                log("[%s] 加载历史数据失败: %s" % (self.name, e))
        try:
            self._file_pos = os.path.getsize(self.data_file) if os.path.exists(self.data_file) else 0
        except OSError:
            self._file_pos = 0

    def _load_read_marks(self):
        try:
            if os.path.exists(self.read_state_file):
                with open(self.read_state_file, "r", encoding="utf-8") as f:
                    self._read_marks = json.load(f)
        except Exception:
            self._read_marks = {}

    def _save_read_marks(self):
        try:
            with open(self.read_state_file, "w", encoding="utf-8") as f:
                json.dump(self._read_marks, f, ensure_ascii=False)
        except Exception as e:
            log("[%s] 保存已读状态失败: %s" % (self.name, e))

    # ---- 跨进程增量加载 ----
    def _refresh_locked(self):
        if not os.path.exists(self.data_file):
            return
        try:
            size = os.path.getsize(self.data_file)
        except OSError:
            return
        if size < self._file_pos:
            # 文件被其他实例清空：本地内存一并重置
            self.items = []
            self._file_pos = 0
        if size == self._file_pos:
            return
        try:
            with open(self.data_file, "r", encoding="utf-8") as f:
                f.seek(self._file_pos)
                chunk = f.read()
            self._file_pos = size
        except (OSError, ValueError):
            return
        known = {i["id"] for i in self.items}
        max_id = self.next_id - 1
        for line in chunk.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                it = json.loads(line)
            except ValueError:
                continue
            iid = it.get("id")
            if iid in known:
                continue
            it["read_by"] = self._read_marks.get(str(iid), [])
            self.items.append(it)
            known.add(iid)
            if isinstance(iid, int) and iid > max_id:
                max_id = iid
        self.next_id = max_id + 1

    # ---- 写入 ----
    def _persist(self, item):
        try:
            with open(self.data_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")
            self._file_pos = os.path.getsize(self.data_file)
        except Exception as e:
            log("[%s] 持久化数据失败: %s" % (self.name, e))

    def add(self, item):
        """新增一条数据并唤醒所有等待者，返回带 id/time 的完整条目。"""
        with self.cond:
            item["id"] = self.next_id
            self.next_id += 1
            item["time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            item["read_by"] = []
            self.items.append(item)
            self._persist(item)
            self.cond.notify_all()
        return item

    # ---- 读取 ----
    def fetch_new(self, consumer, wait_seconds=0):
        """取指定消费者未读数据并标记已读；wait_seconds>0 时长轮询等待。"""
        deadline = time.time() + wait_seconds
        with self.cond:
            while True:
                self._refresh_locked()
                msgs = [i for i in self.items if consumer not in i["read_by"]]
                if msgs or time.time() >= deadline:
                    for i in msgs:
                        i["read_by"].append(consumer)
                        self._read_marks.setdefault(str(i["id"]), []).append(consumer)
                    if msgs:
                        self._save_read_marks()
                    return msgs
                remain = deadline - time.time()
                self.cond.wait(timeout=min(1.0, max(0.0, remain)))

    def fetch_after(self, after_id, wait_seconds=0):
        """取 id 大于 after_id 的数据（游标方式，不标记已读），支持长轮询等待。"""
        deadline = time.time() + wait_seconds
        with self.cond:
            while True:
                self._refresh_locked()
                msgs = [i for i in self.items
                        if isinstance(i.get("id"), int) and i["id"] > after_id]
                if msgs or time.time() >= deadline:
                    return msgs
                remain = deadline - time.time()
                self.cond.wait(timeout=min(1.0, max(0.0, remain)))

    def get_all(self, limit=50):
        with self.lock:
            self._refresh_locked()
            if limit <= 0:
                return list(self.items)
            return list(self.items[-limit:])

    def clear(self):
        with self.lock:
            self.items = []
            self._read_marks = {}
            self._file_pos = 0
            try:
                with open(self.data_file, "w", encoding="utf-8"):
                    pass
            except Exception as e:
                log("[%s] 清空持久化文件失败: %s" % (self.name, e))
            try:
                with open(self.read_state_file, "w", encoding="utf-8") as f:
                    json.dump({}, f)
            except Exception as e:
                log("[%s] 清空已读状态失败: %s" % (self.name, e))


STORE = DataStore(DATA_FILE, READ_STATE_FILE, name="data")
RESP_STORE = DataStore(RESP_DATA_FILE, RESP_READ_FILE, name="result")


# ---------------------------------------------------------------- 图片处理
IMG_EXT = {
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/gif": ".gif",
    "image/webp": ".webp",
    "image/bmp": ".bmp",
}
EXT_MIME = {v: k for k, v in IMG_EXT.items()}


def strip_data_uri(data_b64):
    if data_b64.startswith("data:"):
        # data:image/png;base64,xxxx
        try:
            head, data_b64 = data_b64.split(",", 1)
            mime = head[5:].split(";")[0] or "image/png"
            return data_b64, mime
        except ValueError:
            pass
    return data_b64, None


def save_image(data_b64, mime=None):
    data_b64, uri_mime = strip_data_uri(data_b64)
    mime = mime or uri_mime or "image/png"
    if mime not in IMG_EXT:
        mime = "image/png"
    raw = base64.b64decode(data_b64)
    os.makedirs(IMAGE_DIR, exist_ok=True)
    name = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6] + IMG_EXT[mime]
    path = os.path.join(IMAGE_DIR, name)
    with open(path, "wb") as f:
        f.write(raw)
    return name, mime, len(raw)


# ---------------------------------------------------------------- 客户端广播（主动通知）
class ClientPool:
    """已连接的 MCP 客户端回调（SSE 连接 / stdio 输出）。"""

    def __init__(self):
        self.lock = threading.Lock()
        self.clients = []

    def register(self, cb):
        with self.lock:
            self.clients.append(cb)

    def unregister(self, cb):
        with self.lock:
            if cb in self.clients:
                self.clients.remove(cb)

    def broadcast(self, payload):
        with self.lock:
            clients = list(self.clients)
        for cb in clients:
            try:
                cb(payload)
            except Exception:
                self.unregister(cb)


POOL = ClientPool()


def item_summary(it):
    if it["type"] == "text":
        content = it["content"]
        preview = content if len(content) <= 100 else content[:100] + "..."
        return "文本: " + preview
    return "图片: received_images/%s (%s, %d 字节)" % (it["file"], it["mime"], it["size"])


def notify_new_items(items):
    for it in items:
        payload = {
            "jsonrpc": "2.0",
            "method": "notifications/message",
            "params": {
                "level": "info",
                "logger": "data-bridge",
                "data": "收到新数据 #%d [%s] %s" % (it["id"], it["time"], item_summary(it)),
            },
        }
        POOL.broadcast(payload)


# ---------------------------------------------------------------- 工具格式化
def base_url():
    return "http://%s:%d" % (CONFIG.get("http_host", "127.0.0.1"), CONFIG.get("http_port", 8765))


def format_items(msgs):
    lines = []
    for it in msgs:
        head = "#%d [%s]" % (it["id"], it["time"])
        if it["type"] == "text":
            lines.append("%s 文本:\n%s" % (head, it["content"]))
        else:
            lines.append(
                "%s 图片: received_images/%s (类型 %s, %d 字节, 可通过 %s/images/%s 访问)"
                % (head, it["file"], it["mime"], it["size"], base_url(), it["file"])
            )
    return "\n\n".join(lines)


# ---------------------------------------------------------------- MCP 协议处理
TOOLS = [
    {
        "name": "get_new_data",
        "description": "获取接收接口新收到的文本和图片（读取后自动标记为已读）。"
                       "当 wait_seconds>0 且暂无新数据时，会等待新数据到达后再返回。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "wait_seconds": {
                    "type": "integer",
                    "description": "最长等待新数据的秒数(0-15)，0 表示不等待；需要更长等待时请循环多次调用",
                    "default": 0,
                }
            },
        },
    },
    {
        "name": "get_all_data",
        "description": "获取接收接口收到的全部历史数据（含已读）。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "description": "最多返回条数", "default": 50}
            },
        },
    },
    {
        "name": "clear_data",
        "description": "清空已接收的全部数据。",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "send_result",
        "description": "把 AI 的处理结果/回复内容返回给外部程序。外部程序通过 "
                       "GET /responses?after_id=N 接收（after_id 传上次读到的最大编号）。"
                       "当外部程序推送数据过来、AI 处理完后，必须调用本工具把结果发回去。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "content": {"type": "string", "description": "返回给外部程序的文本内容"},
                "images": {
                    "type": "array",
                    "description": "可选，需要一并返回的图片(base64)",
                    "items": {
                        "type": "object",
                        "properties": {
                            "data": {"type": "string", "description": "base64 图片数据"},
                            "mime": {"type": "string", "description": "图片类型，如 image/png"},
                        },
                    },
                },
            },
            "required": ["content"],
        },
    },
]


def run_tool(name, args):
    if name == "get_new_data":
        try:
            wait = int(args.get("wait_seconds") or 0)
        except (TypeError, ValueError):
            wait = 0
        # 上限 15 秒：必须小于 AI 工具客户端的 MCP 调用超时时间，
        # 否则客户端超时断开后，服务端仍会把数据标记为已读，导致数据"丢失"
        wait = max(0, min(15, wait))
        msgs = STORE.fetch_new(DEFAULT_CONSUMER, wait)
        if not msgs:
            return "当前没有新数据。"
        return format_items(msgs)

    if name == "get_all_data":
        try:
            limit = int(args.get("limit") or 50)
        except (TypeError, ValueError):
            limit = 50
        msgs = STORE.get_all(limit)
        if not msgs:
            return "暂无任何数据。"
        return format_items(msgs)

    if name == "clear_data":
        STORE.clear()
        return "已清空全部数据。"

    if name == "send_result":
        content = args.get("content")
        if not isinstance(content, str) or not content.strip():
            raise ValueError("content 不能为空")
        new_items = [{"type": "text", "content": content.strip()}]
        images = args.get("images") or []
        for im in images:
            if isinstance(im, dict) and im.get("data"):
                try:
                    fname, mime, size = save_image(im["data"], im.get("mime"))
                    new_items.append({"type": "image", "file": fname, "mime": mime,
                                      "size": size, "path": os.path.join(IMAGE_DIR, fname)})
                except Exception as e:
                    log("结果图片保存失败: %s" % e)
        added = [RESP_STORE.add(dict(i)) for i in new_items]
        log("已向外部程序返回 %d 条结果（最新 #%d）" % (len(added), added[-1]["id"]))
        return ("结果已返回给外部程序（共 %d 条，最新编号 #%d）。"
                "程序通过 GET %s/responses?after_id=N 读取。" % (len(added), added[-1]["id"], base_url()))

    raise ValueError("未知工具: %s" % name)


def handle_one(msg):
    if not isinstance(msg, dict):
        return {"jsonrpc": "2.0", "id": None,
                "error": {"code": -32600, "message": "无效请求"}}

    method = msg.get("method")
    mid = msg.get("id")

    if method is None:
        # 客户端对服务端请求/通知的响应，忽略
        return None

    def result(res):
        return {"jsonrpc": "2.0", "id": mid, "result": res}

    def err(code, message):
        return {"jsonrpc": "2.0", "id": mid, "error": {"code": code, "message": message}}

    params = msg.get("params") or {}

    if method == "initialize":
        proto = params.get("protocolVersion", "2024-11-05")
        return result({
            "protocolVersion": proto,
            "capabilities": {"tools": {}, "logging": {}},
            "serverInfo": {"name": "gm-data-bridge-mcp", "version": "1.0.0"},
        })

    if method in ("notifications/initialized", "notifications/cancelled"):
        return None

    if method == "ping":
        return result({})

    if method == "tools/list":
        return result({"tools": TOOLS})

    if method == "tools/call":
        name = params.get("name")
        args = params.get("arguments") or {}
        try:
            text = run_tool(name, args)
            return result({"content": [{"type": "text", "text": text}], "isError": False})
        except Exception as e:
            return result({"content": [{"type": "text", "text": "工具执行出错: %s" % e}],
                           "isError": True})

    return err(-32601, "未知方法: %s" % method)


def handle_mcp_message(msg):
    """处理一条（或一批）JSON-RPC 消息，返回响应（可能为 None=纯通知）。"""
    if isinstance(msg, list):
        responses = [r for r in (handle_one(m) for m in msg) if r is not None]
        return responses if responses else None
    return handle_one(msg)


# ---------------------------------------------------------------- stdio 通知器
def stdio_notifier(payload):
    with STDOUT_LOCK:
        sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
        sys.stdout.flush()


# ---------------------------------------------------------------- HTTP 服务
class Handler(BaseHTTPRequestHandler):
    server_version = "DataBridgeMCP/1.0"
    protocol_version = "HTTP/1.1"
    session_id = None

    # ---- 基础 ----
    def log_message(self, fmt, *args):
        log("%s %s" % (self.address_string(), fmt % args))

    def _read_body(self):
        length = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(length) if length > 0 else b""

    def _send(self, status, body_bytes, content_type="application/json", extra=None):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body_bytes)))
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        try:
            self.wfile.write(body_bytes)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _send_json(self, obj, status=200, extra=None):
        self._send(status, json.dumps(obj, ensure_ascii=False).encode("utf-8"),
                   "application/json; charset=utf-8", extra)

    def _auth_ok(self, qs=None):
        key = self.headers.get("X-API-Key") or ""
        if not key:
            auth = self.headers.get("Authorization") or ""
            if auth.startswith("Bearer "):
                key = auth[7:]
        if not key and qs:
            key = (qs.get("api_key") or [""])[0]
        return bool(CONFIG.get("api_key")) and key == CONFIG["api_key"]

    # ---- GET ----
    def do_GET(self):
        parsed = urlparse(self.path)
        path, qs = parsed.path, parse_qs(parsed.query)

        if path == "/health":
            return self._send_json({"ok": True, "service": "data-bridge-mcp",
                                    "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})

        if not self._auth_ok(qs):
            return self._send_json(
                {"error": "未授权：请通过请求头 X-API-Key（或 ?api_key=）携带正确的 api_key"},
                401)

        if path in ("/data", "/messages"):
            only_new = (qs.get("new") or ["0"])[0] in ("1", "true", "yes")
            consumer = (qs.get("consumer") or ["http"])[0]
            if only_new:
                msgs = STORE.fetch_new(consumer, 0)
            else:
                try:
                    limit = int((qs.get("limit") or ["100"])[0])
                except ValueError:
                    limit = 100
                msgs = STORE.get_all(limit)
            return self._send_json({"ok": True, "count": len(msgs), "data": msgs})

        if path in ("/responses", "/results"):
            # AI 返回给外部程序的结果，游标方式读取（不标记已读，可多消费者）
            try:
                after_id = int((qs.get("after_id") or ["0"])[0])
            except ValueError:
                after_id = 0
            try:
                wait = int((qs.get("wait_seconds") or ["0"])[0])
            except ValueError:
                wait = 0
            wait = max(0, min(60, wait))
            msgs = RESP_STORE.fetch_after(after_id, wait)
            return self._send_json({"ok": True, "count": len(msgs), "data": msgs})

        if path.startswith("/images/"):
            name = os.path.basename(path[len("/images/"):])
            fpath = os.path.join(IMAGE_DIR, name)
            if os.path.isfile(fpath):
                ext = os.path.splitext(name)[1].lower()
                mime = EXT_MIME.get(ext, "application/octet-stream")
                with open(fpath, "rb") as f:
                    raw = f.read()
                return self._send(200, raw, mime)
            return self._send_json({"error": "图片不存在"}, 404)

        if path == "/mcp":
            return self._handle_mcp_sse()

        return self._send_json({"error": "未知路径", "paths":
                                ["/receive", "/data", "/responses", "/mcp",
                                 "/images/<file>", "/health"]}, 404)

    # ---- POST ----
    def do_POST(self):
        parsed = urlparse(self.path)
        path, qs = parsed.path, parse_qs(parsed.query)

        if path == "/mcp":
            return self._handle_mcp_post()

        if path in ("/receive", "/api/receive"):
            return self._handle_receive()

        return self._send_json({"error": "未知路径"}, 404)

    def _handle_receive(self):
        body = self._read_body()  # 先读掉请求体，避免 keep-alive 连接残留数据
        if not self._auth_ok():
            return self._send_json(
                {"error": "未授权：请通过请求头 X-API-Key 携带正确的 api_key"}, 401)

        ctype = (self.headers.get("Content-Type") or "").lower()

        new_items = []

        # 原始图片字节直接上传
        if ctype.startswith("image/"):
            b64 = base64.b64encode(body).decode("ascii")
            name, mime, size = save_image(b64, ctype.split(";")[0])
            new_items.append({"type": "image", "file": name, "mime": mime,
                              "size": size, "path": os.path.join(IMAGE_DIR, name)})
        else:
            try:
                data = json.loads(body.decode("utf-8"))
            except Exception:
                # 非 JSON：当作纯文本
                data = {"text": body.decode("utf-8", errors="replace")}

            if not isinstance(data, dict):
                data = {"text": str(data)}

            texts = []
            if isinstance(data.get("text"), str) and data["text"]:
                texts.append(data["text"])
            elif isinstance(data.get("content"), str) and data["content"]:
                texts.append(data["content"])
            if isinstance(data.get("texts"), list):
                texts.extend(t for t in data["texts"] if isinstance(t, str) and t)

            images = []
            img = data.get("image") or data.get("image_base64")
            if isinstance(img, str) and img:
                images.append({"data": img, "mime": data.get("mime")})
            if isinstance(data.get("images"), list):
                for im in data["images"]:
                    if isinstance(im, str) and im:
                        images.append({"data": im, "mime": None})
                    elif isinstance(im, dict) and im.get("data"):
                        images.append({"data": im["data"], "mime": im.get("mime")})

            for t in texts:
                new_items.append({"type": "text", "content": t})
            for im in images:
                try:
                    name, mime, size = save_image(im["data"], im.get("mime"))
                    new_items.append({"type": "image", "file": name, "mime": mime,
                                      "size": size, "path": os.path.join(IMAGE_DIR, name)})
                except Exception as e:
                    log("保存图片失败: %s" % e)

        added = [STORE.add(dict(it)) for it in new_items]
        if added:
            notify_new_items(added)
            log("收到 %d 条新数据" % len(added))

        return self._send_json({
            "ok": True,
            "received": {"text": sum(1 for i in added if i["type"] == "text"),
                         "images": sum(1 for i in added if i["type"] == "image")},
            "items": [{"id": i["id"], "type": i["type"], "time": i["time"]} for i in added],
        })

    # ---- MCP Streamable HTTP ----
    def _handle_mcp_post(self):
        body = self._read_body()  # 先读掉请求体，避免 keep-alive 连接残留数据
        if not self._auth_ok():
            return self._send_json(
                {"error": "未授权：请通过请求头 X-API-Key 携带正确的 api_key"}, 401)

        try:
            msg = json.loads(body.decode("utf-8"))
        except Exception:
            return self._send_json(
                {"jsonrpc": "2.0", "id": None,
                 "error": {"code": -32700, "message": "JSON 解析失败"}}, 400)

        resp = handle_mcp_message(msg)

        if resp is None:
            # 纯通知：202 Accepted，无响应体
            return self._send(202, b"", "application/json")

        extra = {}
        if Handler.session_id:
            extra["Mcp-Session-Id"] = Handler.session_id
        if isinstance(msg, list):
            return self._send_json(resp if isinstance(resp, list) else [resp], 200, extra)
        return self._send_json(resp, 200, extra)

    def _handle_mcp_sse(self):
        """GET /mcp —— SSE 长连接，新数据到达时主动推送 MCP 通知。"""
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        if Handler.session_id:
            self.send_header("Mcp-Session-Id", Handler.session_id)
        self.end_headers()
        log("MCP 客户端已建立 SSE 通知通道")

        write_lock = threading.Lock()

        def cb(payload):
            data = json.dumps(payload, ensure_ascii=False)
            with write_lock:
                self.wfile.write(("event: message\ndata: " + data + "\n\n").encode("utf-8"))
                self.wfile.flush()

        POOL.register(cb)
        try:
            while True:
                time.sleep(15)
                with write_lock:
                    self.wfile.write(b": ping\n\n")
                    self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            POOL.unregister(cb)
            log("MCP 客户端 SSE 通知通道已断开")


# ---------------------------------------------------------------- 启动入口
def start_http_server():
    Handler.session_id = uuid.uuid4().hex
    host, port = CONFIG.get("http_host", "127.0.0.1"), int(CONFIG.get("http_port", 8765))
    try:
        httpd = ThreadingHTTPServer((host, port), Handler)
    except OSError as e:
        log("HTTP 服务启动失败(端口 %s 可能被占用): %s" % (port, e))
        return None
    httpd.daemon_threads = True
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    return httpd


def print_startup_info():
    key = CONFIG.get("api_key", "")
    info = [
        "=" * 56,
        "  MCP 数据收发服务已启动",
        "-" * 56,
        "  接收接口(其他程序推送数据):",
        "    POST %s/receive" % base_url(),
        '    请求头: X-API-Key: %s' % key,
        '    文本: {"text": "你好"}',
        '    图片: {"images": [{"data": "<base64>", "mime": "image/png"}]}',
        "-" * 56,
        "  MCP 接口(AI 工具连接):",
        "    URL: %s/mcp" % base_url(),
        "    请求头: X-API-Key: %s" % key,
        "  数据查询接口:",
        "    GET %s/data" % base_url(),
        "-" * 56,
        "  结果接收接口(接收 AI 的返回，游标方式):",
        "    GET %s/responses?after_id=0" % base_url(),
        "    可选参数: wait_seconds=0-60(长轮询等待新结果), api_key=<key>",
        "=" * 56,
    ]
    try:
        print("\n".join(info))
    except UnicodeEncodeError:
        for line in info:
            sys.stdout.write(line.encode("gbk", errors="replace").decode("gbk") + "\n")
    sys.stdout.flush()


def run_http_mode():
    POOL.register(lambda payload: log("通知 -> %s" % payload["params"]["data"]))
    httpd = start_http_server()
    if httpd is None:
        log("无法启动 HTTP 服务，退出。")
        return 1
    print_startup_info()
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        log("收到停止信号，正在关闭...")
    httpd.shutdown()
    return 0


def run_stdio_mode():
    """MCP stdio 传输模式（供 CodeBuddy 等以命令行方式注册到 AI 工具时使用）。"""
    # 管道模式下 Windows 默认编码为 GBK，MCP 协议要求 UTF-8
    try:
        sys.stdin.reconfigure(encoding="utf-8")
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    log("MCP stdio 模式已启动")
    POOL.register(stdio_notifier)
    start_http_server()  # 同时提供 HTTP 接收能力（端口被占用时自动跳过）
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except Exception:
            continue
        resp = handle_mcp_message(msg)
        if resp is not None:
            with STDOUT_LOCK:
                sys.stdout.write(json.dumps(resp, ensure_ascii=False) + "\n")
                sys.stdout.flush()
    # stdin 已关闭：稍等片刻让在途的 HTTP 请求完成响应，再退出进程
    time.sleep(2)
    return 0


def main():
    parser = argparse.ArgumentParser(description="MCP 数据收发服务")
    parser.add_argument("--stdio", action="store_true", help="以 MCP stdio 传输模式运行")
    parser.add_argument("--port", type=int, default=None, help="覆盖 HTTP 端口")
    args = parser.parse_args()

    global CONFIG
    CONFIG = ensure_config()
    if args.port:
        CONFIG["http_port"] = args.port

    if args.stdio:
        sys.exit(run_stdio_mode())
    sys.exit(run_http_mode())


if __name__ == "__main__":
    main()
