@echo off
chcp 936 >nul
title MCP 数据收发服务
cd /d "%~dp0"
echo ==============================================
echo   MCP 数据收发服务 启动中...
echo ==============================================
where python >nul 2>nul
if %errorlevel%==0 (
    python mcp_service.py
) else (
    py mcp_service.py
)
echo.
echo 服务已停止，按任意键关闭窗口。
pause >nul
