#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据推送测试工具（带 UI，双向）

左栏：向 MCP 数据收发服务推送文本/图片（模拟外部程序）
右栏：实时显示 AI 工具（CodeBuddy）通过 send_result 工具返回的结果

测试步骤：
1. 确保 MCP 服务已启动（双击 run.bat，或 CodeBuddy 已通过 MCP 配置启动它）
2. 在 CodeBuddy 的 Craft 模式对 AI 说：循环调用 get_new_data（wait_seconds=15）等新数据
3. 在本工具左栏发送文本或图片
4. AI 处理后应调用 send_result 把结果发回，本工具右栏会实时显示
"""

import base64
import json
import os
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime
from tkinter import (BOTTOM, BOTH, Button, END, Entry, Frame, Label, LEFT, RIGHT,
                     PhotoImage, Scrollbar, StringVar, Text, Tk, X, Y,
                     filedialog, messagebox)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config.json")


def load_config():
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception:
        cfg = {"api_key": "", "http_host": "127.0.0.1", "http_port": 8765}
    return cfg


def http_post(url, key, data, content_type):
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", content_type)
    req.add_header("X-API-Key", key)
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


def http_get(url, key):
    req = urllib.request.Request(url, method="GET")
    req.add_header("X-API-Key", key)
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


class TesterApp:
    def __init__(self, root):
        self.root = root
        root.title("MCP 数据收发测试工具（双向）")
        root.geometry("1080x640")
        root.minsize(900, 560)

        cfg = load_config()
        self.base_url = StringVar(
            value="http://%s:%d" % (cfg.get("http_host", "127.0.0.1"),
                                    cfg.get("http_port", 8765)))
        self.api_key = StringVar(value=cfg.get("api_key", ""))
        self.image_path = None
        self.preview_img = None  # 防止预览图被垃圾回收

        # AI 返回数据拉取游标
        self._resp_last_id = 0
        self._resp_error_shown = False

        self._build_ui()
        self._update_combo_state()
        self._start_resp_polling()

    # ---------- UI ----------
    def _build_ui(self):
        main = Frame(self.root)
        main.pack(fill=BOTH, expand=True)

        # ===== 左栏：推送到 AI =====
        left = Frame(main)
        left.pack(side=LEFT, fill=BOTH, expand=True, padx=(10, 5), pady=(10, 6))

        top = Frame(left)
        top.pack(fill=X, pady=(0, 4))
        Label(top, text="base_url:").grid(row=0, column=0, sticky="w")
        Label(top, text="api_key:").grid(row=1, column=0, sticky="w")
        Entry(top, textvariable=self.base_url, width=52).grid(row=0, column=1, padx=6, pady=2, sticky="w")
        Entry(top, textvariable=self.api_key, width=52).grid(row=1, column=1, padx=6, pady=2, sticky="w")

        btns = Frame(left)
        btns.pack(fill=X, pady=4)
        Button(btns, text="测试连接", width=12, command=self.on_test_conn).pack(side=LEFT, padx=2)
        Button(btns, text="查看服务端数据", width=14, command=self.on_view_data).pack(side=LEFT, padx=2)
        Button(btns, text="清空服务端数据", width=14, command=self.on_clear_data).pack(side=LEFT, padx=2)

        send = Frame(left)
        send.pack(fill=X, pady=8)
        Label(send, text="文本内容:").pack(anchor="w")
        self.text_box = Text(send, height=4)
        self.text_box.pack(fill=X, pady=2)
        self.text_box.insert(END, "你好，这是一条来自测试工具的中文消息，当前时间 "
                             + datetime.now().strftime("%H:%M:%S"))
        # 文本变化时刷新"发送图+文"按钮的可用状态
        self.text_box.bind("<<Modified>>", self._on_text_modified)

        row = Frame(send)
        row.pack(fill=X, pady=4)
        Button(row, text="发送文本", width=12, command=self.on_send_text).pack(side=LEFT, padx=2)
        Button(row, text="选择图片...", width=12, command=self.on_pick_image).pack(side=LEFT, padx=2)
        Button(row, text="发送图片", width=12, command=self.on_send_image).pack(side=LEFT, padx=2)
        self.combo_btn = Button(row, text="发送图+文", width=12, command=self.on_send_combo,
                                state="disabled")
        self.combo_btn.pack(side=LEFT, padx=2)
        self.img_label = Label(row, text="（未选择图片）", fg="gray")
        self.img_label.pack(side=LEFT, padx=8)
        self.preview_label = Label(send)
        self.preview_label.pack(anchor="w")

        Label(left, text="日志：").pack(anchor="w")
        logf = Frame(left)
        logf.pack(fill=BOTH, expand=True)
        sb = Scrollbar(logf)
        sb.pack(side=RIGHT, fill=Y)
        self.log_box = Text(logf, height=10, yscrollcommand=sb.set, state="disabled",
                            font=("Consolas", 9))
        self.log_box.pack(side=LEFT, fill=BOTH, expand=True)
        sb.config(command=self.log_box.yview)

        tip = ("提示：MCP 通知不会自动唤醒 AI，需要 AI 主动调用工具读取。\n"
               "推荐流程：在 CodeBuddy 里对 AI 说“循环调用 get_new_data（wait_seconds=15）等新数据”，\n"
               "AI 处理完后会调用 send_result 把结果发回，右栏会实时显示。")
        Label(left, text=tip, fg="#666", justify="left").pack(anchor="w", pady=(2, 0))

        # ===== 右栏：AI 工具返回数据 =====
        right = Frame(main)
        right.pack(side=RIGHT, fill=BOTH, expand=False, padx=(5, 10), pady=(10, 6))

        head = Frame(right)
        head.pack(fill=X)
        Label(head, text="AI 工具返回数据（实时）",
              font=("Microsoft YaHei", 10, "bold")).pack(side=LEFT)
        Button(head, text="清空显示", width=8,
               command=self.on_clear_resp_view).pack(side=RIGHT)

        Label(right, text="来源：AI 调用 send_result 工具的结果，每 2 秒自动拉取",
              fg="#666", justify="left").pack(anchor="w")

        respf = Frame(right)
        respf.pack(fill=BOTH, expand=True, pady=(4, 0))
        rsb = Scrollbar(respf)
        rsb.pack(side=RIGHT, fill=Y)
        self.resp_box = Text(respf, width=44, yscrollcommand=rsb.set, state="disabled",
                             font=("Microsoft YaHei", 9))
        self.resp_box.pack(side=LEFT, fill=BOTH, expand=True)
        rsb.config(command=self.resp_box.yview)

    def log(self, msg):
        self.log_box.config(state="normal")
        self.log_box.insert(END, "[%s] %s\n" % (datetime.now().strftime("%H:%M:%S"), msg))
        self.log_box.see(END)
        self.log_box.config(state="disabled")

    # ---------- "发送图+文"按钮状态 ----------
    def _on_text_modified(self, _event):
        # <<Modified>> 是粘性标志，处理后需复位，否则只触发一次
        self.text_box.edit_modified(False)
        self._update_combo_state()

    def _update_combo_state(self):
        """同时具备文本和图片时，'发送图+文'才可用。"""
        has_text = bool(self.text_box.get("1.0", END).strip())
        has_image = self.image_path is not None
        self.combo_btn.config(state="normal" if (has_text and has_image) else "disabled")

    # ---------- 右栏：AI 返回数据轮询 ----------
    def _start_resp_polling(self):
        threading.Thread(target=self._resp_poll_loop, daemon=True).start()

    def _resp_poll_loop(self):
        """后台线程：游标方式增量拉取 AI 返回结果（每轮长轮询 5 秒）。"""
        while True:
            try:
                url = "%s/responses?after_id=%d&wait_seconds=5" % (
                    self.base_url.get(), self._resp_last_id)
                r = http_get(url, self.api_key.get())
                if not self._resp_error_shown:
                    self._ui_log_text("√ 已连上 AI 返回数据通道，开始监听...\n")
                    self._resp_error_shown = False
                self._resp_error_shown = False
                for it in r.get("data", []):
                    if it["id"] > self._resp_last_id:
                        self._resp_last_id = it["id"]
                    self._show_resp_item(it)
            except Exception as e:
                if not self._resp_error_shown:
                    self._ui_log_text("⚠ 暂未连上 AI 返回数据通道：%s\n  （服务未启动或 api_key 不对，会自动重试）\n" % e)
                    self._resp_error_shown = True
                time.sleep(3)
                continue
            time.sleep(0.5)

    def _show_resp_item(self, it):
        if it["type"] == "text":
            body = it["content"]
        else:
            body = "图片: received_images/%s (%s, %d 字节)" % (it["file"], it["mime"], it["size"])
        self._ui_log_text("──── #%d [%s] ────\n%s\n\n" % (it["id"], it["time"], body))

    def _ui_log_text(self, text):
        """线程安全地把文本追加到右栏。"""
        self.root.after(0, self._append_resp, text)

    def _append_resp(self, text):
        self.resp_box.config(state="normal")
        self.resp_box.insert(END, text)
        self.resp_box.see(END)
        self.resp_box.config(state="disabled")

    def on_clear_resp_view(self):
        self.resp_box.config(state="normal")
        self.resp_box.delete("1.0", END)
        self.resp_box.config(state="disabled")

    # ---------- 动作 ----------
    def _run_async(self, fn):
        threading.Thread(target=fn, daemon=True).start()

    def on_test_conn(self):
        self.log("正在测试连接 %s ..." % self.base_url.get())
        self._run_async(self._test_conn)

    def _test_conn(self):
        try:
            r = http_get(self.base_url.get() + "/health", self.api_key.get())
            self.log("连接成功：服务在线 %s" % r)
        except urllib.error.HTTPError as e:
            self.log("连接失败：HTTP %d（api_key 可能不正确）" % e.code)
        except Exception as e:
            self.log("连接失败：%s（服务可能未启动）" % e)

    def on_send_text(self):
        text = self.text_box.get("1.0", END).strip()
        if not text:
            messagebox.showwarning("提示", "请先输入文本内容")
            return
        payload = json.dumps({"text": text}, ensure_ascii=False).encode("utf-8")
        self.log("正在发送文本（%d 字）..." % len(text))
        self._run_async(lambda: self._post(payload, "application/json; charset=utf-8", "文本"))

    def on_pick_image(self):
        path = filedialog.askopenfilename(
            title="选择图片",
            filetypes=[("图片", "*.png *.jpg *.jpeg *.gif *.webp *.bmp"), ("所有文件", "*.*")])
        if not path:
            return
        self.image_path = path
        size_kb = os.path.getsize(path) / 1024
        self.img_label.config(text="%s (%.1f KB)" % (os.path.basename(path), size_kb), fg="black")
        # 尝试预览（Tk 原生支持 PNG/GIF）
        try:
            img = PhotoImage(file=path)
            if img.width() > 200:
                img = img.subsample(max(1, img.width() // 200))
            self.preview_img = img
            self.preview_label.config(image=img)
        except Exception:
            self.preview_label.config(image="")
            self.preview_img = None
        self._update_combo_state()

    def on_send_image(self):
        if not self.image_path:
            messagebox.showwarning("提示", "请先选择图片")
            return
        try:
            with open(self.image_path, "rb") as f:
                raw = f.read()
        except Exception as e:
            messagebox.showerror("错误", "读取图片失败：%s" % e)
            return
        payload = json.dumps({
            "images": [{"data": base64.b64encode(raw).decode("ascii"),
                        "mime": self._guess_mime(self.image_path)}]
        }).encode("utf-8")
        self.log("正在发送图片 %s（%.1f KB）..." % (os.path.basename(self.image_path),
                                                  len(raw) / 1024))
        self._run_async(lambda: self._post(payload, "application/json", "图片"))

    def on_send_combo(self):
        """一键发送图片 + 文本（同一请求）。"""
        text = self.text_box.get("1.0", END).strip()
        if not text or not self.image_path:
            messagebox.showwarning("提示", "需要同时具备文本内容和图片")
            return
        try:
            with open(self.image_path, "rb") as f:
                raw = f.read()
        except Exception as e:
            messagebox.showerror("错误", "读取图片失败：%s" % e)
            return
        payload = json.dumps({
            "text": text,
            "images": [{"data": base64.b64encode(raw).decode("ascii"),
                        "mime": self._guess_mime(self.image_path)}]
        }).encode("utf-8")
        self.log("正在发送 图+文（文本 %d 字 + 图片 %s %.1f KB）..."
                 % (len(text), os.path.basename(self.image_path), len(raw) / 1024))
        self._run_async(lambda: self._post(payload, "application/json", "图+文"))

    @staticmethod
    def _guess_mime(path):
        ext = os.path.splitext(path)[1].lower()
        return {".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
                ".gif": "image/gif", ".webp": "image/webp", ".bmp": "image/bmp"}.get(ext, "image/png")

    def _post(self, payload, ctype, what):
        try:
            r = http_post(self.base_url.get() + "/receive", self.api_key.get(), payload, ctype)
            if r.get("ok"):
                ids = ",".join("#%s" % i.get("id", "?") for i in r.get("items", [])) or "?"
                self.log("%s 发送成功 -> 编号 %s，等待 AI 处理后返回结果（见右栏）" % (what, ids))
            else:
                self.log("%s 发送失败：%s" % (what, r))
        except urllib.error.HTTPError as e:
            self.log("%s 发送失败：HTTP %d（api_key 可能不正确）" % (what, e.code))
        except Exception as e:
            self.log("%s 发送失败：%s" % (what, e))

    def on_view_data(self):
        self._run_async(self._view_data)

    def _view_data(self):
        try:
            r = http_get(self.base_url.get() + "/data?limit=20", self.api_key.get())
            self.log("服务端共 %d 条数据：" % r["count"])
            for it in r["data"]:
                desc = it["content"][:50] if it["type"] == "text" else it.get("file", "?")
                self.log("  #%s [%s] %s: %s" % (it["id"], it["time"], it["type"], desc))
        except Exception as e:
            self.log("查询失败：%s" % e)

    def on_clear_data(self):
        if not messagebox.askyesno("确认", "确定清空服务端全部数据？"):
            return
        self._run_async(self._clear_data)

    def _clear_data(self):
        try:
            body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                               "params": {"name": "clear_data", "arguments": {}}}).encode("utf-8")
            r = http_post(self.base_url.get() + "/mcp", self.api_key.get(), body, "application/json")
            self.log("清空结果：%s" % r["result"]["content"][0]["text"])
        except Exception as e:
            self.log("清空失败：%s" % e)


def main():
    root = Tk()
    TesterApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
