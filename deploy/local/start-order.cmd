@echo off
chcp 65001 >nul
cd /d "%~dp0"

echo ========================================
echo 启动订单场景基础设施
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
echo 请在 IDEA 中依次启动：
echo 1. BaseDataApplication
echo 2. UserApplication
echo 3. ProgramApplication
echo 4. OrderApplication
echo 5. GatewayApplication
echo.
echo 只有测试支付时才启动 PayApplication。
echo 不需要启动 AdminApplication 和 CustomizeApplication。
echo.

pause