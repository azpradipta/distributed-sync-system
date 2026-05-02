# Deployment Guide — Distributed Sync System

## Prerequisites

| Tool | Version | Instalasi |
|------|---------|-----------|
| Python | 3.8+ | [python.org](https://python.org) |
| Docker Desktop | 24+ | [docs.docker.com](https://docs.docker.com/get-docker/) |
| Redis | 7+ | Via Docker (recommended) |
| Git | any | [git-scm.com](https://git-scm.com) |

---

## 1. Local Development (Without Docker)

### 1.1 Setup Environment

```bash
cd distributed-sync-system
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate

pip install -r requirements.txt
```

### 1.2 Start Redis

```bash
# Using Docker (simplest)
docker run -d --name redis -p 6379:6379 redis:7-alpine

# Or use local Redis if installed
redis-server
```

### 1.3 Configure Environment

```bash
# Copy template
copy .env.example .env    # Windows
cp .env.example .env      # Linux/Mac

# Edit .env jika perlu mengubah port atau API key
```

### 1.4 Start 3 Nodes (3 Terminal Windows)

**Terminal 1 (Node 1 — port 8001):**
```bash
set NODE_ID=node1
set NODE_PORT=8001
set PEER_NODES=http://localhost:8002,http://localhost:8003
set REDIS_HOST=localhost
python main.py
```

**Terminal 2 (Node 2 — port 8002):**
```bash
set NODE_ID=node2
set NODE_PORT=8002
set PEER_NODES=http://localhost:8001,http://localhost:8003
set REDIS_HOST=localhost
python main.py
```

**Terminal 3 (Node 3 — port 8003):**
```bash
set NODE_ID=node3
set NODE_PORT=8003
set PEER_NODES=http://localhost:8001,http://localhost:8002
set REDIS_HOST=localhost
python main.py
```

### 1.5 Verify Cluster

```bash
# Cek health semua node
curl http://localhost:8001/health
curl http://localhost:8002/health
curl http://localhost:8003/health

# Cek Raft status (siapa leader?)
curl http://localhost:8001/raft/status
```

---

## 2. Docker Deployment (Recommended)

### 2.1 Build dan Start

```bash
cd distributed-sync-system

# Build image dan start semua services
docker compose -f docker/docker-compose.yml up --build

# Background mode
docker compose -f docker/docker-compose.yml up --build -d
```

### 2.2 Scaling Nodes

```bash
# Scale ke 5 nodes (perlu update PEER_NODES env)
docker compose -f docker/docker-compose.yml up --scale node=5
```

### 2.3 Monitor Logs

```bash
# Semua nodes
docker compose -f docker/docker-compose.yml logs -f

# Node tertentu
docker logs dss-node1 -f
docker logs dss-node2 -f
docker logs dss-node3 -f
```

### 2.4 Stop

```bash
docker compose -f docker/docker-compose.yml down

# Dengan hapus volumes
docker compose -f docker/docker-compose.yml down -v
```

---

## 3. API Usage Examples

### 3.1 Distributed Lock

```bash
# Header wajib untuk semua request
HEADERS=(-H "X-API-Key: dev-secret-key-change-in-prod" -H "Content-Type: application/json")

# Acquire exclusive lock
curl -X POST http://localhost:8001/lock/acquire "${HEADERS[@]}" \
  -d '{"lock_id": "database-1", "client_id": "service-A", "lock_type": "exclusive"}'

# Acquire shared lock
curl -X POST http://localhost:8001/lock/acquire "${HEADERS[@]}" \
  -d '{"lock_id": "config-1", "client_id": "reader-1", "lock_type": "shared"}'

# Release lock
curl -X POST http://localhost:8001/lock/release "${HEADERS[@]}" \
  -d '{"lock_id": "database-1", "client_id": "service-A"}'

# Check lock status
curl http://localhost:8001/lock/status "${HEADERS[@]}"
```

### 3.2 Distributed Queue

```bash
# Produce message
curl -X POST http://localhost:8001/queue/produce "${HEADERS[@]}" \
  -d '{"queue": "orders", "payload": {"order_id": "ORD-001", "amount": 150000}, "producer_id": "svc-order"}'

# Consume message
curl -X POST http://localhost:8001/queue/consume "${HEADERS[@]}" \
  -d '{"queue": "orders", "consumer_id": "svc-processor"}'

# Acknowledge message (gunakan message_id dari consume response)
curl -X POST http://localhost:8001/queue/ack "${HEADERS[@]}" \
  -d '{"queue": "orders", "message_id": "MSG-UUID-HERE"}'

# Queue status
curl http://localhost:8001/queue/status "${HEADERS[@]}"
```

### 3.3 Distributed Cache (MESI)

```bash
# Write to cache (triggers MESI M-state)
curl -X PUT http://localhost:8001/cache/user:1001 "${HEADERS[@]}" \
  -d '{"value": {"name": "Arya", "role": "admin"}}'

# Read from cache (MESI hit/miss)
curl http://localhost:8001/cache/user:1001 "${HEADERS[@]}"

# Invalidate key
curl -X DELETE http://localhost:8001/cache/user:1001 "${HEADERS[@]}"

# Cache statistics
curl http://localhost:8001/cache/status "${HEADERS[@]}"
```

### 3.4 System Metrics & Audit

```bash
# System metrics
curl http://localhost:8001/metrics "${HEADERS[@]}"

# Audit logs (last 20)
curl "http://localhost:8001/audit/logs?n=20" "${HEADERS[@]}"

# Verify audit log integrity
curl http://localhost:8001/audit/verify "${HEADERS[@]}"
```

---

## 4. Running Tests

```bash
# Install test dependencies
pip install pytest pytest-asyncio

# Run all unit tests
pytest tests/unit/ -v

# Run with coverage
pytest tests/unit/ -v --tb=short

# Run specific test file
pytest tests/unit/test_raft.py -v
pytest tests/unit/test_cache.py -v
pytest tests/unit/test_lock_manager.py -v
```

---

## 5. Performance Benchmarks

```bash
# Run built-in benchmark (node harus running)
python benchmarks/load_test_scenarios.py --host http://localhost:8001

# Locust load test (Web UI di http://localhost:8089)
locust -f tests/performance/locustfile.py --host=http://localhost:8001

# Locust headless (50 users, 10 spawn/s, 60 detik)
locust -f tests/performance/locustfile.py \
  --host=http://localhost:8001 \
  --headless -u 50 -r 10 --run-time 60s \
  --csv=benchmarks/results/perf_$(date +%Y%m%d)
```

---

## 6. Troubleshooting

### Node tidak bisa start

```bash
# Cek apakah port sudah dipakai
netstat -an | findstr :8001   # Windows
lsof -i :8001                 # Linux/Mac

# Cek Redis connection
redis-cli ping
```

### Tidak ada leader yang terpilih

- Pastikan ketiga node bisa saling berkomunikasi (ping/curl antar node)
- Cek firewall/network rules
- Pastikan `PEER_NODES` env var diset dengan benar
- Tunggu 300-600ms untuk election pertama

### Message tidak ter-consume

- Pastikan Redis running: `redis-cli ping`
- Cek queue status: `curl /queue/status`
- Cek apakah message masuk pending list: `redis-cli LLEN queue:NAMA:pending`

### Cache selalu miss

- Normal jika node baru start (cold cache)
- Pastikan peers dapat dijangkau untuk peer-fetch
- Cek cache status: `curl /cache/status`

### Docker build gagal

```bash
# Clean build (tanpa cache)
docker compose -f docker/docker-compose.yml build --no-cache

# Cek Docker disk space
docker system df
docker system prune -f
```
