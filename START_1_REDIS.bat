@echo off
chcp 65001 >nul
echo Memulai Redis via Docker...
docker run -d --name redis -p 6379:6379 redis:7-alpine
echo.
echo Verifikasi Redis:
docker ps --filter "name=redis"
echo.
echo Redis sudah berjalan di port 6379
pause
