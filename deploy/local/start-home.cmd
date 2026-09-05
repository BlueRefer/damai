@echo off
chcp 65001 >nul
cd /d "%~dp0"

echo ========================================
echo 启动首页场景基础设施
echo MySQL Redis Nacos Kafka Elasticsearch
echo ========================================

docker compose -f docker-compose.infra.yml up -d mysql redis nacos kafka elasticsearch

if errorlevel 1 (
    echo.
    echo 基础设施启动失败，请检查 Docker Desktop。
    pause
    exit /b 1
)

echo.
docker compose -f docker-compose.infra.yml ps

echo.
echo 请等待 Elasticsearch 变为 healthy。
echo.
echo 请在 IDEA 中依次启动：
echo 1. BaseDataApplication
echo 2. ProgramApplication
echo 3. GatewayApplication
echo.
echo 前端启动：
echo cd /d D:\Code\damai\vue3
echo npm run dev
echo.
echo 首页：
echo http://127.0.0.1:5173/index
echo.

pause