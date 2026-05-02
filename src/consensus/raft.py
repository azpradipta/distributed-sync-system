"""
Raft Consensus Algorithm Implementation
=======================================
Referensi: "In Search of an Understandable Consensus Algorithm"
           Diego Ongaro & John Ousterhout, USENIX ATC 2014

States    : Follower → Candidate → Leader
Persistent: currentTerm, votedFor, log[]
Volatile  : commitIndex, lastApplied
Leader    : nextIndex[], matchIndex[]

Communication: HTTP/JSON RPC via aiohttp
"""
import asyncio
import logging
import random
import time
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from src.communication.message_passing import MessagePassing
from src.utils.metrics import metrics

logger = logging.getLogger(__name__)


# ── Data Structures ───────────────────────────────────────────────────────────

class NodeState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class LogEntry:
    term: int
    index: int
    command: Dict[str, Any]

    def to_dict(self) -> dict:
        return {"term": self.term, "index": self.index, "command": self.command}

    @classmethod
    def from_dict(cls, d: dict) -> "LogEntry":
        return cls(term=d["term"], index=d["index"], command=d["command"])


# ── Raft Node ─────────────────────────────────────────────────────────────────

class RaftNode:
    """
    Full implementation of the Raft consensus protocol.

    Lifecycle:
        node = RaftNode(node_id, peers, apply_callback)
        await node.start()
        ...
        await node.stop()

    Client Interface:
        result = await node.append_command({"type": "SET", "key": "x", "value": 1})
    """

    def __init__(
        self,
        node_id: str,
        peers: List[str],
        election_timeout_min: float = 0.15,
        election_timeout_max: float = 0.30,
        heartbeat_interval: float = 0.05,
        apply_callback: Optional[Callable] = None,
    ):
        self.node_id = node_id
        self.peers = peers                          # ["http://node2:8002", ...]
        self.election_timeout_min = election_timeout_min
        self.election_timeout_max = election_timeout_max
        self.heartbeat_interval = heartbeat_interval
        self.apply_callback = apply_callback        # async fn(entry: LogEntry)

        # ── Persistent state (simplified; production → Redis/disk) ────────
        self.current_term: int = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []

        # ── Volatile state ────────────────────────────────────────────────
        self.commit_index: int = -1
        self.last_applied: int = -1

        # ── Leader state (re-initialised on election win) ─────────────────
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

        # ── Runtime state ─────────────────────────────────────────────────
        self.state: NodeState = NodeState.FOLLOWER
        self.current_leader: Optional[str] = None
        self.votes_received: set = set()
        self.last_heartbeat: float = time.time()

        # ── Async internals ───────────────────────────────────────────────
        self._lock = asyncio.Lock()
        self._mp = MessagePassing(node_id)
        self._election_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._apply_task: Optional[asyncio.Task] = None
        self._apply_event = asyncio.Event()

        # Pending client futures: log_index → Future
        self._pending: Dict[int, asyncio.Future] = {}

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        await self._mp.start()
        self._election_task = asyncio.create_task(self._election_timer_loop())
        self._apply_task = asyncio.create_task(self._apply_loop())
        logger.info(f"[{self.node_id}] Raft started (peers={self.peers})")

    async def stop(self) -> None:
        for t in [self._election_task, self._heartbeat_task, self._apply_task]:
            if t:
                t.cancel()
        await self._mp.stop()
        logger.info(f"[{self.node_id}] Raft stopped")

    # ── Election Timer (Follower / Candidate) ─────────────────────────────────

    async def _election_timer_loop(self) -> None:
        while True:
            try:
                timeout = random.uniform(self.election_timeout_min, self.election_timeout_max)
                await asyncio.sleep(timeout)
                async with self._lock:
                    if self.state == NodeState.LEADER:
                        continue
                    if time.time() - self.last_heartbeat >= timeout:
                        logger.info(f"[{self.node_id}] Election timeout → starting election")
                        await self._start_election()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(f"[{self.node_id}] Election timer error: {exc}")

    async def _start_election(self) -> None:
        """Become Candidate and request votes. Must hold self._lock."""
        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}
        self.last_heartbeat = time.time()
        metrics.counter("raft_elections_total").increment()
        metrics.gauge("raft_term").set(self.current_term)

        term = self.current_term
        last_idx = len(self.log) - 1
        last_term = self.log[-1].term if self.log else 0

        logger.info(f"[{self.node_id}] Candidate term={term}")

        # Fire vote requests concurrently (release lock while waiting)
        self._lock.release()
        try:
            results = await self._mp.broadcast(
                self.peers,
                "/raft/vote",
                {
                    "term": term,
                    "candidate_id": self.node_id,
                    "last_log_index": last_idx,
                    "last_log_term": last_term,
                },
            )
        finally:
            await self._lock.acquire()

        # Process votes
        if self.state != NodeState.CANDIDATE or self.current_term != term:
            return  # State changed while waiting

        for peer, resp in results.items():
            if resp is None:
                continue
            if resp.get("term", 0) > self.current_term:
                await self._step_down(resp["term"])
                return
            if resp.get("vote_granted"):
                self.votes_received.add(peer)

        majority = (len(self.peers) + 1) // 2 + 1
        if len(self.votes_received) >= majority:
            await self._become_leader()

    async def _become_leader(self) -> None:
        """Transition to Leader state. Must hold self._lock."""
        self.state = NodeState.LEADER
        self.current_leader = self.node_id
        logger.info(f"[{self.node_id}] *** BECAME LEADER for term {self.current_term} ***")

        # Initialise leader-volatile state
        next_idx = len(self.log)
        for peer in self.peers:
            self.next_index[peer] = next_idx
            self.match_index[peer] = -1

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def _step_down(self, new_term: int) -> None:
        """Revert to Follower with a newer term. Must hold self._lock."""
        self.current_term = new_term
        self.voted_for = None
        self.state = NodeState.FOLLOWER
        self.last_heartbeat = time.time()
        metrics.gauge("raft_term").set(new_term)
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            self._heartbeat_task = None
        logger.info(f"[{self.node_id}] Step down → Follower, term={new_term}")

    # ── Heartbeat / Log Replication (Leader) ──────────────────────────────────

    async def _heartbeat_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.heartbeat_interval)
                async with self._lock:
                    if self.state != NodeState.LEADER:
                        break
                    await self._replicate_to_all()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(f"[{self.node_id}] Heartbeat error: {exc}")

    async def _replicate_to_all(self) -> None:
        """Send AppendEntries to all followers. Must hold self._lock."""
        tasks = [self._replicate_to_peer(peer) for peer in self.peers]
        self._lock.release()
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            await self._lock.acquire()
        await self._advance_commit_index()

    async def _replicate_to_peer(self, peer: str) -> None:
        """Send AppendEntries RPC to a single peer (no lock required)."""
        next_idx = self.next_index.get(peer, len(self.log))
        prev_idx = next_idx - 1
        prev_term = self.log[prev_idx].term if prev_idx >= 0 and self.log else 0
        entries = [e.to_dict() for e in self.log[next_idx:]]

        resp = await self._mp.send(
            peer,
            "/raft/append",
            {
                "term": self.current_term,
                "leader_id": self.node_id,
                "prev_log_index": prev_idx,
                "prev_log_term": prev_term,
                "entries": entries,
                "leader_commit": self.commit_index,
            },
        )

        if resp is None:
            return

        async with self._lock:
            if resp.get("term", 0) > self.current_term:
                await self._step_down(resp["term"])
                return
            if self.state != NodeState.LEADER:
                return
            if resp.get("success"):
                new_match = prev_idx + len(entries)
                self.match_index[peer] = max(self.match_index.get(peer, -1), new_match)
                self.next_index[peer] = new_match + 1
            else:
                # Decrement nextIndex and retry on next heartbeat
                self.next_index[peer] = max(0, next_idx - 1)

    async def _advance_commit_index(self) -> None:
        """Check if any new entries can be committed (majority ack). Must hold self._lock."""
        if self.state != NodeState.LEADER:
            return
        n = len(self.log) - 1
        while n > self.commit_index:
            if self.log[n].term != self.current_term:
                n -= 1
                continue
            acks = 1 + sum(1 for m in self.match_index.values() if m >= n)
            majority = (len(self.peers) + 1) // 2 + 1
            if acks >= majority:
                self.commit_index = n
                logger.debug(f"[{self.node_id}] Committed index={n}")
                metrics.counter("raft_commits_total").increment()
                self._apply_event.set()
                # Resolve pending client futures
                if n in self._pending and not self._pending[n].done():
                    self._pending[n].set_result({"status": "committed", "index": n})
                break
            n -= 1

    # ── State Machine Apply Loop ──────────────────────────────────────────────

    async def _apply_loop(self) -> None:
        """Continuously apply committed log entries to the state machine."""
        while True:
            try:
                await self._apply_event.wait()
                self._apply_event.clear()
                async with self._lock:
                    while self.last_applied < self.commit_index:
                        self.last_applied += 1
                        entry = self.log[self.last_applied]
                if self.apply_callback:
                    await self.apply_callback(entry)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(f"[{self.node_id}] Apply loop error: {exc}")

    # ── RPC Handlers ──────────────────────────────────────────────────────────

    async def handle_request_vote(self, data: dict) -> dict:
        """Handle incoming RequestVote RPC."""
        async with self._lock:
            term = data["term"]
            candidate = data["candidate_id"]
            last_log_index = data["last_log_index"]
            last_log_term = data["last_log_term"]

            if term > self.current_term:
                await self._step_down(term)

            # Grant vote if: term OK, haven't voted (or voted same), candidate log ≥ ours
            my_last_idx = len(self.log) - 1
            my_last_term = self.log[-1].term if self.log else 0
            log_ok = (last_log_term > my_last_term) or (
                last_log_term == my_last_term and last_log_index >= my_last_idx
            )
            can_vote = self.voted_for in (None, candidate)
            grant = term == self.current_term and can_vote and log_ok

            if grant:
                self.voted_for = candidate
                self.last_heartbeat = time.time()
                logger.info(f"[{self.node_id}] Voted for {candidate} in term {term}")

            return {"term": self.current_term, "vote_granted": grant}

    async def handle_append_entries(self, data: dict) -> dict:
        """Handle incoming AppendEntries RPC (heartbeat + log replication)."""
        async with self._lock:
            term = data["term"]
            leader_id = data["leader_id"]
            prev_log_index = data["prev_log_index"]
            prev_log_term = data["prev_log_term"]
            entries = [LogEntry.from_dict(e) for e in data.get("entries", [])]
            leader_commit = data["leader_commit"]

            if term < self.current_term:
                return {"term": self.current_term, "success": False}

            # Valid leader heard from — reset election timer
            self.last_heartbeat = time.time()
            self.current_leader = leader_id
            if term > self.current_term:
                await self._step_down(term)
            elif self.state == NodeState.CANDIDATE:
                self.state = NodeState.FOLLOWER

            # Check log consistency
            if prev_log_index >= 0:
                if len(self.log) <= prev_log_index:
                    return {"term": self.current_term, "success": False}
                if self.log[prev_log_index].term != prev_log_term:
                    self.log = self.log[:prev_log_index]
                    return {"term": self.current_term, "success": False}

            # Append new entries
            insert_at = prev_log_index + 1
            for i, entry in enumerate(entries):
                idx = insert_at + i
                if idx < len(self.log):
                    if self.log[idx].term != entry.term:
                        self.log = self.log[:idx]
                        self.log.append(entry)
                else:
                    self.log.append(entry)
            metrics.gauge("raft_log_size").set(len(self.log))

            # Advance commit index
            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, len(self.log) - 1)
                self._apply_event.set()

            return {"term": self.current_term, "success": True}

    # ── Client Interface ──────────────────────────────────────────────────────

    async def append_command(self, command: Dict[str, Any], timeout: float = 5.0) -> dict:
        """
        Submit a command to the Raft cluster.
        Only the leader can accept commands; followers redirect.

        Returns:
            {"status": "committed", "index": N}  on success
            {"status": "not_leader", "leader": URL}  if not leader
        """
        async with self._lock:
            if self.state != NodeState.LEADER:
                return {"status": "not_leader", "leader": self.current_leader}

            entry = LogEntry(
                term=self.current_term,
                index=len(self.log),
                command=command,
            )
            self.log.append(entry)
            metrics.gauge("raft_log_size").set(len(self.log))

            fut: asyncio.Future = asyncio.get_event_loop().create_future()
            self._pending[entry.index] = fut

        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            return {"status": "timeout", "index": entry.index}

    # ── Status ────────────────────────────────────────────────────────────────

    def get_status(self) -> dict:
        return {
            "node_id": self.node_id,
            "state": self.state.value,
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "leader": self.current_leader,
            "log_size": len(self.log),
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "peers": self.peers,
        }
