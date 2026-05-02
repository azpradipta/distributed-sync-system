# Distributed Sync System — Architecture Documentation

## 1. System Overview

Sistem ini mengimplementasikan infrastruktur sinkronisasi terdistribusi yang terdiri dari tiga komponen utama yang saling terintegrasi: Distributed Lock Manager, Distributed Queue, dan Distributed Cache dengan protokol koherensi MESI.

## 2. Arsitektur High-Level

```
┌─────────────────────────────────────────────────────────────────┐
│                    CLIENT / REST API                             │
│               (curl, Python, Postman, etc.)                      │
└────────────────────────┬────────────────────────────────────────┘
                         │ HTTP/JSON (X-API-Key auth)
         ┌───────────────┼───────────────┐
         ▼               ▼               ▼
  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
  │  NODE 1     │  │  NODE 2     │  │  NODE 3     │
  │ :8001       │  │ :8002       │  │ :8003       │
  │─────────────│  │─────────────│  │─────────────│
  │ Raft State  │◄►│ Raft State  │◄►│ Raft State  │
  │ Lock Mgr    │  │ Lock Mgr    │  │ Lock Mgr    │
  │ Queue Node  │  │ Queue Node  │  │ Queue Node  │
  │ Cache Node  │  │ Cache Node  │  │ Cache Node  │
  │ Audit Log   │  │ Audit Log   │  │ Audit Log   │
  └─────────────┘  └─────────────┘  └─────────────┘
         │               │               │
         └───────────────┼───────────────┘
                         ▼
                  ┌─────────────┐
                  │   REDIS     │
                  │  :6379      │
                  │ (Persistence│
                  │  & Queue)   │
                  └─────────────┘
```

## 3. Komponen Utama

### 3.1 Raft Consensus Algorithm

**File:** `src/consensus/raft.py`

Raft adalah algoritma consensus yang dirancang agar mudah dipahami (understandable). Implementasi mencakup:

#### States
| State | Deskripsi |
|-------|-----------|
| FOLLOWER | State awal; menerima heartbeat dari Leader |
| CANDIDATE | Mengumpulkan votes untuk menjadi Leader |
| LEADER | Mengelola replikasi log dan menerima client request |

#### Leader Election
```
Follower (timeout) → Candidate
  ├── Increment term
  ├── Vote for self
  ├── Send RequestVote RPC to all peers
  └── If majority votes → Leader
```
- Election timeout: **150–300ms** (randomized untuk mencegah split vote)
- Heartbeat interval: **50ms**

#### Log Replication
```
Client → Leader.append_command()
  ├── Append entry to leader log
  ├── Send AppendEntries to all followers
  ├── If majority ACK → commit
  └── Apply to state machine
```

#### Safety Properties
1. **Election Safety**: Paling banyak 1 leader per term
2. **Log Matching**: Jika dua log memiliki entry dengan term dan index sama, semua entry sebelumnya identik
3. **Leader Completeness**: Committed entry tidak pernah hilang

### 3.2 Distributed Lock Manager

**File:** `src/nodes/lock_manager.py`

#### Lock Types
- **SHARED (Read)**: Multiple clients dapat mempegang lock secara bersamaan. Digunakan untuk concurrent read.
- **EXCLUSIVE (Write)**: Hanya satu client yang dapat mempegang lock. Blocks semua request lain.

#### State Machine
Semua operasi lock (acquire/release) direalisasikan sebagai Raft log entries:
```
Client.acquire() → Raft.append_command(LOCK_ACQUIRE) → Majority committed → Apply to state machine
```

#### Deadlock Detection (Wait-For Graph)
```
Wait-For Graph (WFG):
  Client A → [waiting for] Client B
  Client B → [waiting for] Client C
  Client C → [waiting for] Client A  ← CYCLE = DEADLOCK!

Resolution: Abort youngest request in cycle (preserves maximum work)
```

Algorithm: DFS cycle detection, runs every 5 seconds.

### 3.3 Distributed Queue System

**File:** `src/nodes/queue_node.py`

#### Consistent Hashing Ring
```
Hash Ring (0 → 2^128):
  
  node1 (150 virtual nodes) ──►  positions: [h(node1#0), h(node1#1), ...]
  node2 (150 virtual nodes) ──►  positions: [h(node2#0), h(node2#1), ...]
  node3 (150 virtual nodes) ──►  positions: [h(node3#0), h(node3#1), ...]

  message_id → hash(message_id) → find next clockwise node → route to that node
```

Menggunakan **150 virtual nodes** per physical node untuk distribusi merata.

#### Message Lifecycle
```
PENDING → IN_FLIGHT → COMPLETED
          │
          └── (timeout) → PENDING (retry)
                          │
                          └── (max retries) → FAILED
```

#### At-Least-Once Delivery
- Consumer menerima pesan → status `IN_FLIGHT`, deadline set
- Consumer memanggil `/queue/ack` → status `COMPLETED`
- Jika tidak di-ack dalam timeout → **redelivered** (max 3x retry)

### 3.4 Cache Coherence Protocol (MESI)

**File:** `src/nodes/cache_node.py`

#### MESI States

| State | Modified | Exclusive | Shared | Invalid |
|-------|----------|-----------|--------|---------|
| **Symbol** | M | E | S | I |
| **Data Valid** | ✓ | ✓ | ✓ | ✗ |
| **Only Copy** | ✓ | ✓ | ✗ | — |
| **Dirty** | ✓ | ✗ | ✗ | — |
| **Can Write** | ✓ (silent) | ✓ (silent) | Need broadcast | Need fetch |

#### State Transition Diagram
```
         Read Miss        Write Miss
    I ──────────────► E ──────────────► M
    │                 │                 │
    │  (others read)  │  (others read)  │ Write
    ▼                 ▼                 │
    S ◄───────────────┘    S ──INVAL──► M
                           (broadcast invalidate)
```

#### LRU Cache Replacement
Menggunakan **OrderedDict** sebagai doubly-linked list:
- O(1) get, put, delete
- Most recently used di akhir dict
- Least recently used (head) di-evict saat kapasitas penuh

### 3.5 Security (Bonus D)

**Files:** `security/tls_manager.py`, `security/audit_logger.py`

#### RBAC (Role-Based Access Control)
| Role | Permissions |
|------|-------------|
| admin | Semua operasi |
| producer | queue:write |
| consumer | queue:read, queue:write (untuk ack) |
| reader | cache:read, lock:read, metrics:read |

API key dikirim via `X-API-Key` header.

#### Tamper-Evident Audit Log
```
Entry N: {data} + prev_hash(N-1) → SHA256 → hash(N)
Entry N+1: {data} + hash(N) → SHA256 → hash(N+1)
...

Modifying any entry breaks the hash chain → tampering detected!
```

## 4. Communication Layer

### Inter-Node RPC
- **Protocol**: HTTP/JSON via aiohttp
- **Timeout**: 500ms per RPC call
- **Retry**: Raft handles retry via heartbeat cycle
- **Failure Detection**: Phi Accrual-inspired heartbeat monitor

### Failure Handling Scenarios

| Scenario | Handling |
|----------|----------|
| Leader crash | Followers detect timeout → new election (150-300ms) |
| Follower crash | Leader continues replicating to remaining nodes; re-syncs on recovery |
| Network partition | Minority partition cannot commit (no majority) |
| Lock holder crash | Lock timeout auto-release (configurable, default 30s) |
| Queue node crash | Redis persistence ensures no message loss; consistent hashing re-routes |
| Cache node crash | MESI invalidation ensures no stale data; cache miss triggers fresh fetch |

## 5. Data Flow Examples

### Lock Acquire Flow
```
Client → POST /lock/acquire → LockManager.acquire()
  → Raft.append_command(LOCK_ACQUIRE)
  → Leader appends to log
  → Replicates to node2, node3
  → Majority ACK → commit
  → apply_callback(_on_commit)
  → LockState.can_acquire_exclusive() → grant or queue
  → Response to client
```

### Message Produce Flow
```
Client → POST /queue/produce → QueueNode.produce()
  → hash(message_id) → consistent_hash_ring.get_node()
  → If self: store to Redis
  → If peer: forward via HTTP → peer stores to Redis
  → Response: {"status": "produced", "message_id": "..."}
```

### Cache Write Flow (MESI)
```
Client → PUT /cache/{key} → CacheNode.write()
  → Check current state
  → If S or I: broadcast INVALIDATE to all peers
    → Peers: set state to INVALID
  → Set local state to MODIFIED
  → Store new value
  → Response: {"status": "written", "state": "M"}
```

## 6. Performance Characteristics

| Component | Throughput | Latency (p50) | Notes |
|-----------|-----------|----------------|-------|
| Lock Acquire | ~50-100 req/s | 10-50ms | Bounded by Raft round-trip |
| Queue Produce | ~200-500 req/s | 5-20ms | Redis write + consistent hash |
| Cache Read (hit) | ~1000+ req/s | < 1ms | In-memory LRU |
| Cache Read (miss) | ~100-300 req/s | 5-15ms | Peer fetch + state transition |

*Values measured on single machine with 3 local nodes.*
