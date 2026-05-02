"""
Distributed Lock Manager
========================
Implements shared/exclusive locks using Raft consensus.
All lock operations replicated through Raft log for strong consistency.

Lock Types:
    SHARED    (read)  : multiple holders allowed
    EXCLUSIVE (write) : single holder, blocks all others

Deadlock Detection: Wait-For Graph with DFS cycle detection.
"""
import asyncio
import logging
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Set

from src.consensus.raft import LogEntry
from src.nodes.base_node import BaseNode
from src.utils.config import config
from src.utils.metrics import metrics

logger = logging.getLogger(__name__)


class LockType(Enum):
    SHARED = "shared"
    EXCLUSIVE = "exclusive"


@dataclass
class LockRequest:
    request_id: str
    lock_id: str
    client_id: str
    lock_type: LockType
    timestamp: float = field(default_factory=time.time)
    timeout: float = 30.0


@dataclass
class LockState:
    lock_id: str
    shared_holders: Set[str] = field(default_factory=set)
    exclusive_holder: Optional[str] = None
    waiting_queue: List[LockRequest] = field(default_factory=list)

    def is_free(self) -> bool:
        return not self.shared_holders and self.exclusive_holder is None

    def can_acquire_shared(self) -> bool:
        return self.exclusive_holder is None

    def can_acquire_exclusive(self) -> bool:
        return self.is_free()


class LockManager(BaseNode):
    """Distributed Lock Manager backed by Raft consensus."""

    def __init__(self):
        super().__init__(use_raft=True)
        self._locks: Dict[str, LockState] = {}
        self._client_requests: Dict[str, LockRequest] = {}
        self._wait_for: Dict[str, Set[str]] = defaultdict(set)
        self._lock = asyncio.Lock()
        self._deadlock_task: Optional[asyncio.Task] = None

    async def _on_start(self) -> None:
        self._deadlock_task = asyncio.create_task(self._deadlock_detection_loop())

    async def _on_stop(self) -> None:
        if self._deadlock_task:
            self._deadlock_task.cancel()

    # ── Public API ────────────────────────────────────────────────────────────

    async def acquire(self, lock_id: str, client_id: str, lock_type: str = "exclusive", timeout: float = None) -> dict:
        """Acquire a distributed lock via Raft consensus."""
        req_id = str(uuid.uuid4())
        result = await self.raft.append_command({
            "type": "LOCK_ACQUIRE",
            "request_id": req_id,
            "lock_id": lock_id,
            "client_id": client_id,
            "lock_type": lock_type,
            "timeout": timeout or config.LOCK_TIMEOUT,
            "timestamp": time.time(),
        })
        if result.get("status") == "not_leader":
            return {"status": "error", "reason": "not_leader", "leader": result.get("leader")}
        metrics.counter("lock_acquire_total").increment()
        return {"status": "acquired", "request_id": req_id, "lock_id": lock_id}

    async def release(self, lock_id: str, client_id: str) -> dict:
        """Release a lock via Raft consensus."""
        result = await self.raft.append_command({
            "type": "LOCK_RELEASE",
            "lock_id": lock_id,
            "client_id": client_id,
        })
        if result.get("status") == "not_leader":
            return {"status": "error", "reason": "not_leader"}
        metrics.counter("lock_release_total").increment()
        return {"status": "released", "lock_id": lock_id}

    def get_status(self) -> dict:
        return {
            "node": self.node_id,
            "locks": {
                lid: {
                    "exclusive_holder": ls.exclusive_holder,
                    "shared_holders": list(ls.shared_holders),
                    "queue_depth": len(ls.waiting_queue),
                }
                for lid, ls in self._locks.items()
            },
            "active_locks": int(metrics.gauge("active_locks").value),
            "raft": self.raft.get_status(),
        }

    # ── Raft State Machine ────────────────────────────────────────────────────

    async def _on_commit(self, entry: LogEntry) -> None:
        cmd = entry.command
        async with self._lock:
            if cmd.get("type") == "LOCK_ACQUIRE":
                await self._apply_acquire(cmd)
            elif cmd.get("type") == "LOCK_RELEASE":
                await self._apply_release(cmd)

    async def _apply_acquire(self, cmd: dict) -> None:
        lock_id = cmd["lock_id"]
        client_id = cmd["client_id"]
        lt = LockType(cmd["lock_type"])
        req = LockRequest(
            request_id=cmd["request_id"],
            lock_id=lock_id,
            client_id=client_id,
            lock_type=lt,
            timestamp=cmd["timestamp"],
            timeout=cmd["timeout"],
        )
        if lock_id not in self._locks:
            self._locks[lock_id] = LockState(lock_id=lock_id)
        ls = self._locks[lock_id]

        if lt == LockType.SHARED and ls.can_acquire_shared():
            ls.shared_holders.add(client_id)
            metrics.gauge("active_locks").increment()
        elif lt == LockType.EXCLUSIVE and ls.can_acquire_exclusive():
            ls.exclusive_holder = client_id
            metrics.gauge("active_locks").increment()
        else:
            ls.waiting_queue.append(req)
            self._client_requests[req.request_id] = req
            holders = ls.shared_holders | ({ls.exclusive_holder} if ls.exclusive_holder else set())
            self._wait_for[client_id].update(holders)

    async def _apply_release(self, cmd: dict) -> None:
        lock_id = cmd["lock_id"]
        client_id = cmd["client_id"]
        ls = self._locks.get(lock_id)
        if not ls:
            return
        if ls.exclusive_holder == client_id:
            ls.exclusive_holder = None
            metrics.gauge("active_locks").decrement()
        ls.shared_holders.discard(client_id)
        self._wait_for.pop(client_id, None)
        for w in self._wait_for.values():
            w.discard(client_id)
        # Grant next in queue
        still = []
        for req in ls.waiting_queue:
            if req.lock_type == LockType.EXCLUSIVE and ls.can_acquire_exclusive():
                ls.exclusive_holder = req.client_id
                metrics.gauge("active_locks").increment()
                break
            elif req.lock_type == LockType.SHARED and ls.can_acquire_shared():
                ls.shared_holders.add(req.client_id)
                metrics.gauge("active_locks").increment()
            else:
                still.append(req)
        ls.waiting_queue = still

    # ── Deadlock Detection (Wait-For Graph) ───────────────────────────────────

    async def _deadlock_detection_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(config.DEADLOCK_CHECK_INTERVAL)
                async with self._lock:
                    cycles = self._find_cycles()
                    for cycle in cycles:
                        metrics.counter("deadlock_detected_total").increment()
                        logger.warning(f"[{self.node_id}] DEADLOCK: {cycle}")
                        await self._resolve_deadlock(cycle)
            except asyncio.CancelledError:
                break

    def _find_cycles(self) -> List[List[str]]:
        """DFS on Wait-For Graph to detect cycles."""
        visited: Set[str] = set()
        rec: Set[str] = set()
        cycles: List[List[str]] = []

        def dfs(node: str, path: List[str]) -> None:
            visited.add(node)
            rec.add(node)
            path.append(node)
            for nb in self._wait_for.get(node, set()):
                if nb not in visited:
                    dfs(nb, path)
                elif nb in rec:
                    cycles.append(path[path.index(nb):])
            path.pop()
            rec.discard(node)

        for c in list(self._wait_for):
            if c not in visited:
                dfs(c, [])
        return cycles

    async def _resolve_deadlock(self, cycle: List[str]) -> None:
        """Abort youngest request in cycle to break deadlock."""
        victim = max(
            (r for r in self._client_requests.values() if r.client_id in cycle),
            key=lambda r: r.timestamp,
            default=None,
        )
        if victim:
            logger.warning(f"Deadlock resolution: aborting {victim.client_id}")
            for ls in self._locks.values():
                if ls.exclusive_holder == victim.client_id:
                    await self._apply_release({"lock_id": ls.lock_id, "client_id": victim.client_id})
                ls.shared_holders.discard(victim.client_id)
