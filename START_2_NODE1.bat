@echo off
chcp 65001 >nul
set PYTHONUTF8=1
cd /d c:\tugas3sister\distributed-sync-system
if not exist logs mkdir logs
set NODE_ID=node1
set NODE_PORT=8001
set PEER_NODES=http://localhost:8002,http://localhost:8003
set REDIS_HOST=localhost
set API_KEY=dev-secret-key-change-in-prod
set LOG_LEVEL=INFO
set ELECTION_TIMEOUT_MIN=1.5
set ELECTION_TIMEOUT_MAX=3.0
set HEARTBEAT_INTERVAL=0.5
echo ============================================
echo  NODE 1 - Port 8001
echo ============================================
python main.py
pause
