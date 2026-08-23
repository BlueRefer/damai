@echo off
chcp 65001 >nul
cd /d "%~dp0"

echo 请先在 IDEA 中停止所有 Java 服务。
echo 请先在前端终端按 Ctrl+C。
echo.
echo 正在安全停止 Docker 基础设施……

docker compose -f docker-compose.infra.yml stop

echo.
docker compose -f docker-compose.infra.yml ps

echo.
echo 容器已经停止，数据卷未删除。
echo 不要使用 docker compose down -v。
echo.

pause