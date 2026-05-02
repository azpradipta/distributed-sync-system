# Distributed Sync System

> **Tugas 3 - Sistem Parallel dan Terdistribusi**
> Implementasi Distributed Synchronization System
>
> **Nama**: Arya Zaky Pradipta
> **NIM**: 11231013

---

## Overview

Sistem sinkronisasi terdistribusi yang mengimplementasikan:

| Komponen | Algoritma | Poin |
|----------|-----------|------|
| 🔒 **Distributed Lock Manager** | Raft Consensus + Wait-For Graph | 25 poin |
| 📬 **Distributed Queue** | Consistent Hashing + Redis | 20 poin |
| 🗄️ **Cache Coherence** | MESI Protocol + LRU | 15 poin |
| 🐳 **Containerization** | Docker + Docker Compose | 10 poin |
| 🔐 **Security (Bonus D)** | TLS + RBAC + Audit Log | +5 poin |

## Architecture

```
Client ──► Node1 (Leader*) ◄──► Node2 ◄──► Node3
               │                               │
               └────────── Redis ──────────────┘
                         (Persistence)
```

3 nodes berkomunikasi via HTTP/JSON. Raft consensus memastikan consistency.

## Quick Start

### Docker (Recommended)

```bash
# Clone & start
git clone <repo-url>
cd distributed-sync-system
docker compose -f docker/docker-compose.yml up --build

# Test health
curl http://localhost:8001/health
```

### Local (3 Terminals)

```bash
pip install -r requirements.txt

# Terminal 1
set NODE_ID=node1 && set NODE_PORT=8001 && set PEER_NODES=http://localhost:8002,http://localhost:8003 && python main.py

# Terminal 2
set NODE_ID=node2 && set NODE_PORT=8002 && set PEER_NODES=http://localhost:8001,http://localhost:8003 && python main.py

# Terminal 3
set NODE_ID=node3 && set NODE_PORT=8003 && set PEER_NODES=http://localhost:8001,http://localhost:8002 && python main.py
```

## API Endpoints

All endpoints require `X-API-Key: dev-secret-key-change-in-prod` header.

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Node health check |
| GET | `/raft/status` | Raft consensus state |
| POST | `/lock/acquire` | Acquire distributed lock |
| POST | `/lock/release` | Release distributed lock |
| GET | `/lock/status` | Current lock table |
| POST | `/queue/produce` | Produce a message |
| POST | `/queue/consume` | Consume a message |
| POST | `/queue/ack` | Acknowledge message |
| GET | `/cache/{key}` | Read from cache (MESI) |
| PUT | `/cache/{key}` | Write to cache (MESI) |
| DELETE | `/cache/{key}` | Invalidate cache key |
| GET | `/metrics` | System metrics |
| GET | `/audit/logs` | Audit log entries |
| GET | `/audit/verify` | Verify audit log integrity |

## Demo Commands

```bash
HEADER='-H "X-API-Key: dev-secret-key-change-in-prod" -H "Content-Type: application/json"'

# 1. Lock: Acquire exclusive
curl -X POST http://localhost:8001/lock/acquire $HEADER \
  -d '{"lock_id":"db-1","client_id":"svc-A","lock_type":"exclusive"}'

# 2. Queue: Produce & Consume
curl -X POST http://localhost:8001/queue/produce $HEADER \
  -d '{"queue":"orders","payload":{"id":1,"amount":150000}}'
curl -X POST http://localhost:8001/queue/consume $HEADER \
  -d '{"queue":"orders","consumer_id":"worker-1"}'

# 3. Cache: Write (M-state) & Read (hit)
curl -X PUT http://localhost:8001/cache/user:1 $HEADER \
  -d '{"value":{"name":"Arya","role":"admin"}}'
curl http://localhost:8001/cache/user:1 $HEADER
```

## Tests

```bash
# Unit tests
pytest tests/unit/ -v

# Benchmark (node must be running)
python benchmarks/load_test_scenarios.py

# Load test (Locust UI at localhost:8089)
locust -f tests/performance/locustfile.py --host=http://localhost:8001
```

## Project Structure

```
distributed-sync-system/
├── src/
│   ├── consensus/raft.py          # Raft algorithm
│   ├── nodes/
│   │   ├── base_node.py           # Abstract base
│   │   ├── lock_manager.py        # Distributed Lock + Deadlock Detection
│   │   ├── queue_node.py          # Consistent Hash Queue + Redis
│   │   └── cache_node.py          # MESI Cache Coherence + LRU
│   ├── communication/
│   │   ├── message_passing.py     # Async HTTP RPC
│   │   └── failure_detector.py    # Heartbeat failure detection
│   └── utils/
│       ├── config.py              # Environment config
│       └── metrics.py             # Metrics collection
├── security/
│   ├── tls_manager.py             # TLS + RBAC (Bonus D)
│   └── audit_logger.py            # Tamper-evident audit log
├── tests/
│   ├── unit/                      # pytest unit tests
│   └── performance/locustfile.py  # Locust load tests
├── docker/
│   ├── Dockerfile.node            # Multi-stage Docker build
│   └── docker-compose.yml         # 3-node cluster orchestration
├── docs/
│   ├── architecture.md            # System architecture
│   ├── api_spec.yaml              # OpenAPI 3.0 spec
│   └── deployment_guide.md        # Setup & deployment guide
├── benchmarks/
│   └── load_test_scenarios.py     # Performance benchmarks
├── main.py                        # aiohttp REST API server
├── requirements.txt
└── .env.example
```

## Documentation

- 📐 [Architecture](docs/architecture.md)
- 📋 [API Spec](docs/api_spec.yaml)
- 🚀 [Deployment Guide](docs/deployment_guide.md)

## Video Demo

> Link YouTube: *[akan diisi setelah recording]*

## Tech Stack

- **Python 3.11** + asyncio
- **aiohttp** — Async HTTP server & client
- **Redis** — Message persistence & distributed state
- **Docker** + Docker Compose — Containerization
- **pytest** + locust — Testing & load testing
- **cryptography** — TLS certificate generation (Bonus D)
