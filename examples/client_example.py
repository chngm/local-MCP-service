# -*- coding: utf-8 -*-
"""
MCP 数据收发服务 —— Python 客户端例程（仅标准库，Python 3.7+）

功能：
1. send_text(text)                推送文本
2. send_image(image_path)         推送图片（自动 base64）
3. send_text_image(text, path)    图+文一次推送
4. receive_results(after_id, wait_seconds)  接收 AI 返回的结果（游标 + 长轮询）

运行：python client_example.py
"""
import base64
import json
import os
import sys
import urllib.error
import urllib.request

# ===== 按需修改这三项 =====
BASE_URL = "http://127.0.0.1:8765"
API_KEY = "dk_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"   # 运行 mcp_service.py 后从 config.json 读取，或看启动窗口输出
TIMEOUT = 15                                       # 常规请求超时（秒）


# ---------------------------------------------------------------- 基础请求
def _request(method, path, body=None, timeout=TIMEOUT):
    url = BASE_URL + path
    data = None
    if body is not None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json; charset=utf-8")
    req.add_header("X-API-Key", API_KEY)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


# ---------------------------------------------------------------- 发送数据
def send_text(text):
    """推送一条文本，返回 {"ok":true, "items":[{id,...}]}"""
    return _request("POST", "/receive", {"text": text})


def send_image(image_path, mime=None):
    """推送一张图片（自动转 base64）"""
    if mime is None:
        ext = os.path.splitext(image_path)[1].lower()
        mime = {".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
                ".gif": "image/gif", ".webp": "image/webp", ".bmp": "image/bmp",
                }.get(ext, "image/png")
    with open(image_path, "rb") as f:
        raw = f.read()
    return _request("POST", "/receive", {
        "images": [{"data": base64.b64encode(raw).decode("ascii"), "mime": mime}]
    })


def send_text_image(text, image_path, mime=None):
    """图 + 文一次推送（AI 一次 get_new_data 就能全部读到）"""
    if mime is None:
        ext = os.path.splitext(image_path)[1].lower()
        mime = {".png": "image/png", ".jpg": "image/jpeg",
                ".jpeg": "image/jpeg"}.get(ext, "image/png")
    with open(image_path, "rb") as f:
        raw = f.read()
    return _request("POST", "/receive", {
        "text": text,
        "images": [{"data": base64.b64encode(raw).decode("ascii"), "mime": mime}]
    })


# ---------------------------------------------------------------- 接收 AI 结果
def receive_results(after_id, wait_seconds=0):
    """
    游标方式接收 AI 通过 send_result 返回的结果。
    :param after_id:     上次读到的最大结果编号（首次传 0）
    :param wait_seconds: 0-60，没有新结果时服务端等待多久再返回（长轮询）
    :return: {"ok":true, "count":n, "data":[{id,time,type,content},...]}
    """
    wait = max(0, min(60, int(wait_seconds)))
    return _request("GET", "/responses?after_id=%d&wait_seconds=%d" % (after_id, wait),
                    timeout=wait + TIMEOUT)


# ---------------------------------------------------------------- 使用示例
def main():
    # 1. 推送文本
    r = send_text("你好 AI，请告诉我今天的日期")
    print("发送文本:", r)
    first_id = r["items"][0]["id"] if r.get("items") else None

    # 2. 推送图片（有图片时取消注释）
    # r = send_image(r"D:\test\demo.png")
    # print("发送图片:", r)

    # 3. 图+文一起推（有需要时取消注释）
    # r = send_text_image("这张图里有什么？", r"D:\test\demo.png")
    # print("发送图+文:", r)

    # 4. 循环接收 AI 的返回结果（游标 + 10 秒长轮询）
    after_id = 0
    print("开始监听 AI 返回结果（Ctrl+C 退出）...")
    try:
        while True:
            resp = receive_results(after_id, wait_seconds=10)
            for it in resp.get("data", []):
                after_id = max(after_id, it["id"])          # 更新游标
                if it["type"] == "text":
                    print("[AI返回 #%s %s] %s" % (it["id"], it["time"], it["content"]))
                else:
                    print("[AI返回 #%s %s] 图片: received_images/%s" % (it["id"], it["time"], it["file"]))
    except KeyboardInterrupt:
        print("已退出")


if __name__ == "__main__":
    main()
