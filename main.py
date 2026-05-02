"""
Main API Server — Distributed Sync System
==========================================
aiohttp REST API that exposes all distributed system components.

Endpoints:
    GET  /health                - Node health check
    GET  /raft/status           - Raft consensus state
    POST /raft/vote             - [internal] RequestVote RPC
    POST /raft/append           - [internal] AppendEntries RPC

    POST /lock/acquire          - Acquire distributed lock
    POST /lock/release          - Release distributed lock
    GET  /lock/status           - Current lock table

    POST /queue/produce         - Produce a message
    POST /queue/consume         - Consume a message
    POST /queue/ack             - Acknowledge a message
    POST /queue/store           - [internal] Store forwarded message
    GET  /queue/status          - Queue metrics

    GET  /cache/{key}           - Read from distributed cache
    PUT  /cache/{key}           - Write to distributed cache
    DELETE /cache/{key}         - Invalidate cache key
    GET  /cache/peek/{key}      - [internal] Peek without LRU update
    POST /cache/invalidate/{key}- [internal] MESI invalidation RPC
    GET  /cache/status          - Cache metrics

    GET  /metrics               - System-wide metrics
    GET  /audit/logs            - Recent audit log entries
    GET  /audit/verify          - Verify audit log integrity
"""
import asyncio
import logging
import os
import sys

from aiohttp import web

from src.nodes.lock_manager import LockManager
from src.nodes.queue_node import QueueNode
from src.nodes.cache_node import CacheNode
from src.utils.config import config
from src.utils.metrics import metrics
from security.tls_manager import auth_middleware, TLSManager
from security.audit_logger import AuditLogger

# ── Logging Setup ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f"logs/{config.NODE_ID}.log", mode="a"),
    ],
)
logger = logging.getLogger(__name__)

os.makedirs("logs", exist_ok=True)

# ── Global Node Instances ─────────────────────────────────────────────────────

lock_mgr = LockManager()
queue_node = QueueNode()
cache_node = CacheNode()
audit = AuditLogger(config.NODE_ID, config.AUDIT_LOG_FILE)


# ── Route Handlers ────────────────────────────────────────────────────────────

async def health(request: web.Request) -> web.Response:
    return web.json_response({
        "status": "ok",
        "node_id": config.NODE_ID,
        "raft_state": lock_mgr.raft.state.value,
    })


# ── Raft Internal RPCs ────────────────────────────────────────────────────────

async def raft_vote(request: web.Request) -> web.Response:
    data = await request.json()
    result = await lock_mgr.raft.handle_request_vote(data)
    return web.json_response(result)


async def raft_append(request: web.Request) -> web.Response:
    data = await request.json()
    result = await lock_mgr.raft.handle_append_entries(data)
    return web.json_response(result)


async def raft_status(request: web.Request) -> web.Response:
    return web.json_response(lock_mgr.raft.get_status())


# ── Lock Endpoints ────────────────────────────────────────────────────────────

async def lock_acquire(request: web.Request) -> web.Response:
    data = await request.json()
    lock_id = data.get("lock_id", "")
    client_id = data.get("client_id", request.get("client_id", "anon"))
    lock_type = data.get("lock_type", "exclusive")
    timeout = data.get("timeout")
    result = await lock_mgr.acquire(lock_id, client_id, lock_type, timeout)
    await audit.log(client_id, "LOCK_ACQUIRE", lock_id, result.get("status"), data)
    return web.json_response(result)


async def lock_release(request: web.Request) -> web.Response:
    data = await request.json()
    lock_id = data.get("lock_id", "")
    client_id = data.get("client_id", request.get("client_id", "anon"))
    result = await lock_mgr.release(lock_id, client_id)
    await audit.log(client_id, "LOCK_RELEASE", lock_id, result.get("status"))
    return web.json_response(result)


async def lock_status(request: web.Request) -> web.Response:
    return web.json_response(lock_mgr.get_status())


# ── Queue Endpoints ───────────────────────────────────────────────────────────

async def queue_produce(request: web.Request) -> web.Response:
    data = await request.json()
    queue = data.get("queue", "default")
    payload = data.get("payload", data.get("message"))
    producer_id = data.get("producer_id", request.get("client_id", "anon"))
    result = await queue_node.produce(queue, payload, producer_id)
    await audit.log(producer_id, "QUEUE_PRODUCE", queue, "produced", {"message_id": result.get("message_id")})
    return web.json_response(result)


async def queue_consume(request: web.Request) -> web.Response:
    data = await request.json()
    queue = data.get("queue", "default")
    consumer_id = data.get("consumer_id", request.get("client_id", "anon"))
    ack_timeout = data.get("ack_timeout", 30.0)
    result = await queue_node.consume(queue, consumer_id, ack_timeout)
    return web.json_response(result)


async def queue_ack(request: web.Request) -> web.Response:
    data = await request.json()
    queue = data.get("queue", "default")
    message_id = data.get("message_id", "")
    result = await queue_node.acknowledge(queue, message_id)
    return web.json_response(result)


async def queue_store_internal(request: web.Request) -> web.Response:
    """Internal endpoint: receive forwarded message from another node."""
    data = await request.json()
    from src.nodes.queue_node import Message
    msg = Message.from_dict(data)
    await queue_node._store_message(msg)
    return web.json_response({"status": "stored"})


async def queue_status(request: web.Request) -> web.Response:
    result = await queue_node.get_status()
    return web.json_response(result)


# ── Cache Endpoints ───────────────────────────────────────────────────────────

async def cache_read(request: web.Request) -> web.Response:
    key = request.match_info["key"]
    result = await cache_node.read(key)
    return web.json_response(result)


async def cache_write(request: web.Request) -> web.Response:
    key = request.match_info["key"]
    data = await request.json()
    value = data.get("value")
    result = await cache_node.write(key, value)
    client_id = request.get("client_id", "anon")
    await audit.log(client_id, "CACHE_WRITE", key, "written")
    return web.json_response(result)


async def cache_invalidate_endpoint(request: web.Request) -> web.Response:
    key = request.match_info["key"]
    result = await cache_node.invalidate(key)
    return web.json_response(result)


async def cache_peek(request: web.Request) -> web.Response:
    """Internal: peer fetch without affecting LRU."""
    key = request.match_info["key"]
    return web.json_response(cache_node.peek(key))


async def cache_invalidate_rpc(request: web.Request) -> web.Response:
    """Internal: MESI invalidation broadcast from another node."""
    key = request.match_info["key"]
    result = await cache_node.invalidate(key)
    return web.json_response(result)


async def cache_status(request: web.Request) -> web.Response:
    return web.json_response(cache_node.get_status())


# ── Metrics & Audit ───────────────────────────────────────────────────────────

async def get_metrics(request: web.Request) -> web.Response:
    return web.json_response(metrics.get_snapshot())


async def audit_logs(request: web.Request) -> web.Response:
    n = int(request.rel_url.query.get("n", 50))
    return web.json_response(audit.get_recent(n))


async def audit_verify(request: web.Request) -> web.Response:
    return web.json_response(audit.verify_integrity())


# ── App Factory ───────────────────────────────────────────────────────────────

def create_app() -> web.Application:
    app = web.Application(middlewares=[auth_middleware])

    app.router.add_get("/health", health)
    app.router.add_get("/raft/status", raft_status)
    app.router.add_post("/raft/vote", raft_vote)
    app.router.add_post("/raft/append", raft_append)

    app.router.add_post("/lock/acquire", lock_acquire)
    app.router.add_post("/lock/release", lock_release)
    app.router.add_get("/lock/status", lock_status)

    app.router.add_post("/queue/produce", queue_produce)
    app.router.add_post("/queue/consume", queue_consume)
    app.router.add_post("/queue/ack", queue_ack)
    app.router.add_post("/queue/store", queue_store_internal)
    app.router.add_get("/queue/status", queue_status)

    app.router.add_get("/cache/{key}", cache_read)
    app.router.add_put("/cache/{key}", cache_write)
    app.router.add_delete("/cache/{key}", cache_invalidate_endpoint)
    app.router.add_get("/cache/peek/{key}", cache_peek)
    app.router.add_post("/cache/invalidate/{key}", cache_invalidate_rpc)
    app.router.add_get("/cache/status", cache_status)

    app.router.add_get("/metrics", get_metrics)
    app.router.add_get("/audit/logs", audit_logs)
    app.router.add_get("/audit/verify", audit_verify)

    app.on_startup.append(startup)
    app.on_cleanup.append(cleanup)

    return app


async def startup(app: web.Application) -> None:
    logger.info(f"Starting node {config.NODE_ID} on port {config.NODE_PORT}")
    if config.TLS_ENABLED:
        tls = TLSManager(config.TLS_CERT_FILE, config.TLS_KEY_FILE)
        tls.generate_self_signed(config.NODE_ID)
    await lock_mgr.start()
    await queue_node.start()
    await cache_node.start()
    logger.info(f"Node {config.NODE_ID} ready!")


async def cleanup(app: web.Application) -> None:
    logger.info(f"Stopping node {config.NODE_ID}")
    await cache_node.stop()
    await queue_node.stop()
    await lock_mgr.stop()


# ── Entry Point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    ssl_ctx = None
    if config.TLS_ENABLED:
        tls = TLSManager(config.TLS_CERT_FILE, config.TLS_KEY_FILE)
        ssl_ctx = tls.get_ssl_context()

    app = create_app()
    web.run_app(
        app,
        host=config.NODE_HOST,
        port=config.NODE_PORT,
        ssl_context=ssl_ctx,
        access_log=logger,
    )
