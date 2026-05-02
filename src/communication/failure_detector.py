"""
Failure Detector — Heartbeat-based node liveness monitoring.
Uses a Phi Accrual-inspired approach: nodes are suspected after
`threshold` consecutive missed heartbeats.
"""
import asyncio
import logging
import time
from typing import Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class NodeStatus:
    ALIVE = "alive"
    SUSPECTED = "suspected"
    DEAD = "dead"


class FailureDetector:
    """
    Tracks liveness of peer nodes using periodic heartbeats.

    The detector marks a node as SUSPECTED after `suspect_threshold`
    consecutive missed intervals, and DEAD after `dead_threshold`.
    Recovery is immediate on the first successful response.
    """

    def __init__(
        self,
        node_id: str,
        peers: List[str],
        heartbeat_interval: float = 0.5,
        suspect_threshold: int = 3,
        dead_threshold: int = 6,
        ping_callback: Optional[Callable] = None,
        on_suspect: Optional[Callable] = None,
        on_recover: Optional[Callable] = None,
    ):
        self.node_id = node_id
        self.peers = peers
        self.heartbeat_interval = heartbeat_interval
        self.suspect_threshold = suspect_threshold
        self.dead_threshold = dead_threshold

        # Callbacks
        self._ping_callback = ping_callback       # async fn(peer) → bool
        self._on_suspect = on_suspect             # async fn(peer)
        self._on_recover = on_recover             # async fn(peer)

        # Tracking state
        self._miss_counts: Dict[str, int] = {p: 0 for p in peers}
        self._status: Dict[str, str] = {p: NodeStatus.ALIVE for p in peers}
        self._last_seen: Dict[str, float] = {p: time.time() for p in peers}

        self._task: Optional[asyncio.Task] = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._task = asyncio.create_task(self._heartbeat_loop())
        logger.info(f"[{self.node_id}] FailureDetector started, monitoring {len(self.peers)} peers")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info(f"[{self.node_id}] FailureDetector stopped")

    # ── Main Loop ─────────────────────────────────────────────────────────────

    async def _heartbeat_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.heartbeat_interval)
                await asyncio.gather(*[self._probe(peer) for peer in self.peers])
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(f"[{self.node_id}] FailureDetector error: {exc}")

    async def _probe(self, peer: str) -> None:
        """Ping a peer and update its status accordingly."""
        alive = False
        if self._ping_callback:
            try:
                alive = await self._ping_callback(peer)
            except Exception:
                alive = False

        prev_status = self._status[peer]

        if alive:
            self._miss_counts[peer] = 0
            self._last_seen[peer] = time.time()
            if prev_status != NodeStatus.ALIVE:
                self._status[peer] = NodeStatus.ALIVE
                logger.info(f"[{self.node_id}] Node {peer} RECOVERED")
                if self._on_recover:
                    asyncio.create_task(self._on_recover(peer))
        else:
            self._miss_counts[peer] += 1
            misses = self._miss_counts[peer]
            if misses >= self.dead_threshold:
                new_status = NodeStatus.DEAD
            elif misses >= self.suspect_threshold:
                new_status = NodeStatus.SUSPECTED
            else:
                new_status = NodeStatus.ALIVE

            if new_status != prev_status:
                self._status[peer] = new_status
                logger.warning(
                    f"[{self.node_id}] Node {peer} → {new_status} (misses={misses})"
                )
                if new_status == NodeStatus.SUSPECTED and self._on_suspect:
                    asyncio.create_task(self._on_suspect(peer))

    # ── Query API ─────────────────────────────────────────────────────────────

    def is_alive(self, peer: str) -> bool:
        return self._status.get(peer) == NodeStatus.ALIVE

    def alive_peers(self) -> List[str]:
        return [p for p in self.peers if self.is_alive(p)]

    def suspected_peers(self) -> List[str]:
        return [p for p in self.peers if self._status.get(p) == NodeStatus.SUSPECTED]

    def get_status(self) -> Dict[str, dict]:
        now = time.time()
        return {
            peer: {
                "status": self._status[peer],
                "miss_count": self._miss_counts[peer],
                "last_seen_ago": round(now - self._last_seen[peer], 2),
            }
            for peer in self.peers
        }

    def record_alive(self, peer: str) -> None:
        """Externally mark a peer as alive (e.g. from Raft heartbeat)."""
        if peer in self._miss_counts:
            prev = self._status[peer]
            self._miss_counts[peer] = 0
            self._last_seen[peer] = time.time()
            self._status[peer] = NodeStatus.ALIVE
            if prev != NodeStatus.ALIVE and self._on_recover:
                asyncio.create_task(self._on_recover(peer))
