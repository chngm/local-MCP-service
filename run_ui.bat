@echo off
chcp 936 >nul
title MCP 数据推送测试工具
cd /d "%~dp0"
echo ==============================================
echo   MCP 数据推送测试工具 启动中...
echo ==============================================
where python >nul 2>nul
if %errorlevel%==0 (
    start "" pythonw ui_tester.py
) else (
    start "" pyw ui_tester.py
)
