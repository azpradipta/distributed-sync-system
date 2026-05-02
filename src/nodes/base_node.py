"""
Base Node — Abstract foundation shared by all node types.
Provides lifecycle management and shared infrastructure.
"""
import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Optional

from src.communication.failure_detector import FailureDetector
from src.communication.message_passing import MessagePassing
from src.consensus.raft import RaftNode
from src.utils.config import config
from src.utils.metrics import metrics

logger = logging.getLogger(__name__)


class BaseNode(ABC):
    """
    Abstract base for Lock, Queue, and Cache nodes.
    Wires together: Raft, MessagePassing, FailureDetector.
    """

    def __init__(self, use_raft: bool = False):
        self.node_id = config.NODE_ID
        self.peers = config.PEER_NODES
        self.use_raft = use_raft

        # Shared infrastructure
        self.mp = MessagePassing(self.node_id)
        self.raft = None
        if self.use_raft:
            self.raft = RaftNode(
                node_id=self.node_id,
                peers=self.peers,
                election_timeout_min=config.ELECTION_TIMEOUT_MIN,
                election_timeout_max=config.ELECTION_TIMEOUT_MAX,
                heartbeat_interval=config.HEARTBEAT_INTERVAL,
                apply_callback=self._on_commit,
            )
        self.failure_detector = FailureDetector(
            node_id=self.node_id,
            peers=self.peers,
            heartbeat_interval=0.5,
            ping_callback=self._ping_peer,
            on_suspect=self._on_peer_suspect,
            on_recover=self._on_peer_recover,
        )
        self._started = False

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        await self.mp.start()
        if self.use_raft and self.raft:
            await self.raft.start()
        await self.failure_detector.start()
        await self._on_start()
        self._started = True
        logger.info(f"[{self.node_id}] {self.__class__.__name__} started")

    async def stop(self) -> None:
        await self.failure_detector.stop()
        if self.use_raft and self.raft:
            await self.raft.stop()
        await self.mp.stop()
        await self._on_stop()
        self._started = False
        logger.info(f"[{self.node_id}] {self.__class__.__name__} stopped")

    # ── Abstract hooks ────────────────────────────────────────────────────────

    @abstractmethod
    async def _on_commit(self, entry) -> None:
        """Called when a Raft log entry is committed and applied."""

    async def _on_start(self) -> None:
        """Hook called after all infrastructure is started."""

    async def _on_stop(self) -> None:
        """Hook called before infrastructure is stopped."""

    # ── Failure Detector callbacks ────────────────────────────────────────────

    async def _ping_peer(self, peer: str) -> bool:
        resp = await self.mp.send(peer, "/health", {}, method="GET")
        return resp is not None

    async def _on_peer_suspect(self, peer: str) -> None:
        logger.warning(f"[{self.node_id}] Peer {peer} is SUSPECTED")

    async def _on_peer_recover(self, peer: str) -> None:
        logger.info(f"[{self.node_id}] Peer {peer} RECOVERED")

    # ── Helpers ───────────────────────────────────────────────────────────────

    def get_base_status(self) -> dict:
        status = {
            "node_id": self.node_id,
            "peers": self.failure_detector.get_status(),
        }
        if self.use_raft and self.raft:
            status["raft"] = self.raft.get_status()
        return status
