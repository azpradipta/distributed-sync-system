"""
Distributed Queue System
========================
Consistent hashing ring distributes messages across queue nodes.
Supports multiple producers/consumers, message persistence (Redis),
at-least-once delivery, and node failure recovery.

Architecture:
    - Virtual nodes on hash ring for even distribution
    - Each message assigned to a node by hash(message_id)
    - Redis stores pending/in-flight/completed messages
    - Consumer acks trigger deletion; timeout triggers redelivery
"""
import asyncio
import hashlib
import json
import logging
import time
import uuid
from bisect import bisect_left, insort
from dataclasses import dataclass, asdict
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import redis.asyncio as aioredis

from src.nodes.base_node import BaseNode
from src.utils.config import config
from src.utils.metrics import metrics

logger = logging.getLogger(__name__)


class MessageStatus(Enum):
    PENDING = "pending"
    IN_FLIGHT = "in_flight"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class Message:
    message_id: str
    queue: str
    payload: Any
    producer_id: str
    status: str = MessageStatus.PENDING.value
    created_at: float = 0.0
    delivered_at: float = 0.0
    retry_count: int = 0
    ack_deadline: float = 0.0      # epoch time by which consumer must ack

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Message":
        return cls(**d)


# ── Consistent Hash Ring ──────────────────────────────────────────────────────

class ConsistentHashRing:
    """
    Hash ring with virtual nodes for even load distribution.
    Each physical node is represented by VIRTUAL_NODES points on the ring.
    """

    def __init__(self, virtual_nodes: int = 150):
        self.virtual_nodes = virtual_nodes
        self._ring: List[int] = []                 # sorted hash positions
        self._map: Dict[int, str] = {}             # hash → node_id

    def add_node(self, node_id: str) -> None:
        for i in range(self.virtual_nodes):
            h = self._hash(f"{node_id}#{i}")
            insort(self._ring, h)
            self._map[h] = node_id
        logger.debug(f"ConsistentHash: added node {node_id} ({self.virtual_nodes} vnodes)")

    def remove_node(self, node_id: str) -> None:
        for i in range(self.virtual_nodes):
            h = self._hash(f"{node_id}#{i}")
            if h in self._map:
                self._ring.remove(h)
                del self._map[h]
        logger.debug(f"ConsistentHash: removed node {node_id}")

    def get_node(self, key: str) -> Optional[str]:
        if not self._ring:
            return None
        h = self._hash(key)
        idx = bisect_left(self._ring, h)
        if idx == len(self._ring):
            idx = 0
        return self._map[self._ring[idx]]

    @staticmethod
    def _hash(key: str) -> int:
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def __len__(self) -> int:
        return len(set(self._map.values()))


# ── Queue Node ────────────────────────────────────────────────────────────────

class QueueNode(BaseNode):
    """
    Distributed Queue Node using Consistent Hashing.
    Persists messages in Redis; handles producer/consumer lifecycle.
    """

    def __init__(self):
        super().__init__()
        self._redis: Optional[aioredis.Redis] = None
        self._ring = ConsistentHashRing(virtual_nodes=config.VIRTUAL_NODES)
        self._redelivery_task: Optional[asyncio.Task] = None
        self._all_nodes: List[str] = [config.NODE_ID] + [
            self._url_to_id(p) for p in config.PEER_NODES
        ]

    @staticmethod
    def _url_to_id(url: str) -> str:
        """Extract node_id from URL like http://node2:8002 → node2."""
        return url.split("//")[-1].split(":")[0]

    async def _on_start(self) -> None:
        self._redis = aioredis.from_url(
            config.get_redis_url(),
            encoding="utf-8",
            decode_responses=True,
        )
        # Build hash ring
        for nid in self._all_nodes:
            self._ring.add_node(nid)
        logger.info(f"[{self.node_id}] Hash ring: {len(self._ring)} nodes")
        self._redelivery_task = asyncio.create_task(self._redelivery_loop())

    async def _on_stop(self) -> None:
        if self._redelivery_task:
            self._redelivery_task.cancel()
        if self._redis:
            await self._redis.aclose()

    async def _on_commit(self, entry) -> None:
        """Raft commits are not used for queue routing (stateless routing)."""
        pass

    # ── Producer API ──────────────────────────────────────────────────────────

    async def produce(self, queue: str, payload: Any, producer_id: str = "anon") -> dict:
        """
        Produce a message onto the distributed queue.
        Routes to the responsible node via consistent hashing.
        """
        msg = Message(
            message_id=str(uuid.uuid4()),
            queue=queue,
            payload=payload,
            producer_id=producer_id,
            created_at=time.time(),
        )
        target_node = self._ring.get_node(msg.message_id)

        if target_node == self.node_id:
            await self._store_message(msg)
        else:
            # Forward to correct node
            target_url = self._node_id_to_url(target_node)
            resp = await self.mp.send(target_url, "/queue/store", msg.to_dict())
            if resp is None:
                # Fallback: store locally (resilience)
                logger.warning(f"[{self.node_id}] Forward failed → storing locally")
                await self._store_message(msg)

        metrics.counter("messages_produced_total").increment()
        metrics.gauge("queue_depth").increment()
        logger.info(f"[{self.node_id}] Produced {msg.message_id} → queue={queue}")
        return {"status": "produced", "message_id": msg.message_id, "queue": queue}

    async def _store_message(self, msg: Message) -> None:
        """Persist message to Redis."""
        key = f"queue:{msg.queue}:msg:{msg.message_id}"
        await self._redis.hset(key, mapping=self._flatten(msg.to_dict()))
        await self._redis.lpush(f"queue:{msg.queue}:pending", msg.message_id)
        await self._redis.expire(key, 86400)  # 24h TTL

    # ── Consumer API ──────────────────────────────────────────────────────────

    async def consume(self, queue: str, consumer_id: str, ack_timeout: float = 30.0) -> dict:
        """
        Consume one message from the queue (at-least-once delivery).
        Message transitions to IN_FLIGHT until consumer calls ack().
        """
        msg_id = await self._redis.rpop(f"queue:{queue}:pending")
        if not msg_id:
            return {"status": "empty", "queue": queue}

        key = f"queue:{queue}:msg:{msg_id}"
        data = await self._redis.hgetall(key)
        if not data:
            return {"status": "empty", "queue": queue}

        msg = Message.from_dict(self._unflatten(data))
        msg.status = MessageStatus.IN_FLIGHT.value
        msg.delivered_at = time.time()
        msg.ack_deadline = time.time() + ack_timeout

        await self._redis.hset(key, mapping=self._flatten(msg.to_dict()))
        await self._redis.zadd(f"queue:{queue}:in_flight", {msg_id: msg.ack_deadline})

        metrics.counter("messages_consumed_total").increment()
        metrics.gauge("queue_depth").decrement()
        logger.info(f"[{self.node_id}] Consumed {msg_id} by {consumer_id}")
        return {"status": "ok", "message": msg.to_dict()}

    async def acknowledge(self, queue: str, message_id: str) -> dict:
        """Mark a message as successfully processed (removes from in-flight)."""
        key = f"queue:{queue}:msg:{message_id}"
        await self._redis.hset(key, "status", MessageStatus.COMPLETED.value)
        await self._redis.zrem(f"queue:{queue}:in_flight", message_id)
        logger.info(f"[{self.node_id}] ACK {message_id}")
        return {"status": "completed", "message_id": message_id}

    # ── Redelivery Loop (At-Least-Once) ───────────────────────────────────────

    async def _redelivery_loop(self) -> None:
        """Redeliver messages whose ack deadline has expired."""
        while True:
            try:
                await asyncio.sleep(config.MESSAGE_RETRY_INTERVAL)
                await self._check_and_redeliver()
            except asyncio.CancelledError:
                break

    async def _check_and_redeliver(self) -> None:
        now = time.time()
        # Scan all queue in-flight sets
        keys = await self._redis.keys("queue:*:in_flight")
        for key in keys:
            queue = key.split(":")[1]
            expired = await self._redis.zrangebyscore(key, "-inf", now)
            for msg_id in expired:
                msg_key = f"queue:{queue}:msg:{msg_id}"
                data = await self._redis.hgetall(msg_key)
                if not data:
                    continue
                msg = Message.from_dict(self._unflatten(data))
                if msg.retry_count >= config.MESSAGE_MAX_RETRIES:
                    msg.status = MessageStatus.FAILED.value
                    await self._redis.hset(msg_key, mapping=self._flatten(msg.to_dict()))
                    await self._redis.zrem(key, msg_id)
                    logger.error(f"[{self.node_id}] Message {msg_id} FAILED after {msg.retry_count} retries")
                    continue
                # Redeliver
                msg.status = MessageStatus.PENDING.value
                msg.retry_count += 1
                await self._redis.hset(msg_key, mapping=self._flatten(msg.to_dict()))
                await self._redis.zrem(key, msg_id)
                await self._redis.lpush(f"queue:{queue}:pending", msg_id)
                metrics.counter("messages_redelivered_total").increment()
                logger.warning(f"[{self.node_id}] Redelivering {msg_id} (attempt {msg.retry_count})")

    # ── Status ────────────────────────────────────────────────────────────────

    async def get_status(self) -> dict:
        queues = {}
        keys = await self._redis.keys("queue:*:pending")
        for key in keys:
            queue = key.split(":")[1]
            queues[queue] = {
                "pending": await self._redis.llen(key),
                "in_flight": await self._redis.zcard(f"queue:{queue}:in_flight"),
            }
        return {
            "node": self.node_id,
            "ring_nodes": len(self._ring),
            "queues": queues,
            "metrics": {
                "produced": metrics.counter("messages_produced_total").value,
                "consumed": metrics.counter("messages_consumed_total").value,
                "redelivered": metrics.counter("messages_redelivered_total").value,
            },
        }

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _node_id_to_url(self, node_id: str) -> str:
        for peer in config.PEER_NODES:
            if node_id in peer:
                return peer
        return f"http://{node_id}:{config.NODE_PORT}"

    @staticmethod
    def _flatten(d: dict) -> dict:
        """Flatten nested dict for Redis hset (convert all values to str)."""
        return {k: json.dumps(v) if isinstance(v, (dict, list)) else str(v) for k, v in d.items()}

    @staticmethod
    def _unflatten(d: dict) -> dict:
        """Restore types from Redis hgetall strings."""
        result = {}
        for k, v in d.items():
            try:
                result[k] = json.loads(v)
            except (json.JSONDecodeError, TypeError):
                # Try numeric coercion
                try:
                    result[k] = float(v) if "." in v else int(v)
                except (ValueError, TypeError):
                    result[k] = v
        return result
