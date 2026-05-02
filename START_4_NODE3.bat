@echo off
chcp 65001 >nul
set PYTHONUTF8=1
cd /d c:\tugas3sister\distributed-sync-system
if not exist logs mkdir logs
set NODE_ID=node3
set NODE_PORT=8003
set PEER_NODES=http://localhost:8001,http://localhost:8002
set REDIS_HOST=localhost
set API_KEY=dev-secret-key-change-in-prod
set LOG_LEVEL=INFO
echo ============================================
echo  NODE 3 - Port 8003
echo ============================================
python main.py
pause
