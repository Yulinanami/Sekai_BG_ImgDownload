@echo off
chcp 65001 >nul 2>&1
title Sekai 背景图批量下载工具

echo ========================================
echo   Sekai 背景图批量下载工具
echo ========================================
echo.

:: 检查 Node.js
where node >nul 2>&1
if %errorlevel% neq 0 (
    echo [错误] 未检测到 Node.js，请先安装: https://nodejs.org/
    echo.
    pause
    exit /b 1
)

:: 显示 Node.js 版本
for /f "tokens=*" %%v in ('node -v') do echo [√] Node.js 版本: %%v

:: 检查依赖是否已安装
if not exist "node_modules" (
    echo [!] 首次运行，正在安装依赖...
    npm install
    if %errorlevel% neq 0 (
        echo [错误] 依赖安装失败
        pause
        exit /b 1
    )
    echo [√] 依赖安装完成
) else (
    echo [√] 依赖已就绪
)

echo.
echo 开始下载...
echo.

node download.js

echo.
pause
