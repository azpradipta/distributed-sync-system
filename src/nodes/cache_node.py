"""
Distributed Cache with MESI Coherence Protocol
===============================================
Implements the MESI cache coherence protocol across multiple cache nodes.

MESI States:
    M (Modified)  - Data dirty, only in this cache. Must write-back before others read.
    E (Exclusive) - Data clean, only in this cache. Can upgrade to M silently.
    S (Shared)    - Data clean, present in >= 1 cache. No writes allowed directly.
    I (Invalid)   - Data not valid. Must fetch before use.

State Transition Rules:
    Read Hit:     M/E/S → M/E/S   (no state change, return data)
    Read Miss:    I     → E (sole reader) or S (others also have it)
    Write Hit:    M     → M        (write directly)
                  E     → M        (upgrade, no broadcast needed)
                  S     → M        (broadcast INVALIDATE to sharers, then write)
    Write Miss:   I     → M        (fetch + broadcast INVALIDATE)

Cache Replacement: LRU (Least Recently Used) via doubly-linked list + dict.
"""
import asyncio
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set

from src.nodes.base_node import BaseNode
from src.utils.config import config
from src.utils.metrics import metrics

logger = logging.getLogger(__name__)


class MESIState(Enum):
    MODIFIED = "M"
    EXCLUSIVE = "E"
    SHARED = "S"
    INVALID = "I"


@dataclass
class CacheEntry:
    key: str
    value: Any
    state: MESIState
    version: int = 0
    last_access: float = field(default_factory=time.time)
    dirty: bool = False           # True only in Modified state


class LRUCache:
    """
    LRU cache backed by OrderedDict.
    Evicts least-recently-used entry when capacity is exceeded.
    """

    def __init__(self, capacity: int):
        self.capacity = capacity
        self._store: OrderedDict[str, CacheEntry] = OrderedDict()

    def get(self, key: str) -> Optional[CacheEntry]:
        if key not in self._store:
            return None
        self._store.move_to_end(key)    # mark as recently used
        entry = self._store[key]
        entry.last_access = time.time()
        return entry

    def put(self, key: str, entry: CacheEntry) -> Optional[str]:
        """
        Insert/update a cache entry. Returns evicted key if capacity exceeded.
        """
        evicted_key = None
        if key in self._store:
            self._store.move_to_end(key)
        else:
            if len(self._store) >= self.capacity:
                evicted_key, _ = self._store.popitem(last=False)
                metrics.counter("cache_evictions_total").increment()
        self._store[key] = entry
        return evicted_key

    def invalidate(self, key: str) -> bool:
        if key in self._store:
            del self._store[key]
            return True
        return False

    def __len__(self) -> int:
        return len(self._store)

    def __contains__(self, key: str) -> bool:
        return key in self._store

    def all_keys(self) -> List[str]:
        return list(self._store.keys())


class CacheNode(BaseNode):
    """
    Distributed cache node implementing MESI coherence protocol.
    Coordinates state transitions with peer nodes via HTTP RPC.
    """

    def __init__(self):
        super().__init__()
        self._cache = LRUCache(capacity=config.CACHE_SIZE)
        self._lock = asyncio.Lock()

    async def _on_commit(self, entry) -> None:
        """Cache operations are handled via direct RPC, not Raft log."""
        pass

    # ── Read ──────────────────────────────────────────────────────────────────

    async def read(self, key: str) -> dict:
        """
        MESI Read operation.
        Hit:  M/E/S → return cached value.
        Miss: I     → fetch from memory/peers, transition to E or S.
        """
        async with self._lock:
            entry = self._cache.get(key)

            if entry and entry.state != MESIState.INVALID:
                # Cache Hit
                metrics.counter("cache_hits_total").increment()
                logger.debug(f"[{self.node_id}] Cache HIT {key} state={entry.state.value}")
                return {"status": "hit", "key": key, "value": entry.value, "state": entry.state.value}

            # Cache Miss — fetch from peers or memory
            metrics.counter("cache_misses_total").increment()
            logger.debug(f"[{self.node_id}] Cache MISS {key}")

            peer_data = await self._fetch_from_peers(key)

            if peer_data:
                value = peer_data["value"]
                version = peer_data.get("version", 0)
                # Others have it → SHARED
                new_state = MESIState.SHARED
            else:
                # We're the only one — load from backing store (simulated)
                value = await self._load_from_memory(key)
                version = 0
                new_state = MESIState.EXCLUSIVE  # sole holder

            if value is not None:
                new_entry = CacheEntry(key=key, value=value, state=new_state, version=version)
                self._cache.put(key, new_entry)
                metrics.gauge("cache_size").set(len(self._cache))

            return {
                "status": "miss",
                "key": key,
                "value": value,
                "state": new_state.value if value is not None else MESIState.INVALID.value,
            }

    # ── Write ─────────────────────────────────────────────────────────────────

    async def write(self, key: str, value: Any) -> dict:
        """
        MESI Write operation.
        M: write directly.
        E: upgrade to M, write.
        S: broadcast INVALIDATE to all sharers, upgrade to M.
        I: fetch (or create), invalidate others, set M.
        """
        async with self._lock:
            entry = self._cache.get(key)
            old_state = entry.state if entry else MESIState.INVALID

            if old_state == MESIState.MODIFIED:
                # Already exclusive dirty — just write
                entry.value = value
                entry.version += 1
                entry.dirty = True
                logger.debug(f"[{self.node_id}] Write {key}: M→M")

            elif old_state == MESIState.EXCLUSIVE:
                # Silent upgrade, no broadcast needed
                entry.value = value
                entry.state = MESIState.MODIFIED
                entry.version += 1
                entry.dirty = True
                logger.debug(f"[{self.node_id}] Write {key}: E→M")

            elif old_state in (MESIState.SHARED, MESIState.INVALID):
                # Must invalidate all peers
                await self._broadcast_invalidate(key)
                version = (entry.version + 1) if entry else 0
                new_entry = CacheEntry(
                    key=key, value=value,
                    state=MESIState.MODIFIED,
                    version=version,
                    dirty=True,
                )
                self._cache.put(key, new_entry)
                metrics.gauge("cache_size").set(len(self._cache))
                logger.debug(f"[{self.node_id}] Write {key}: {old_state.value}→M (invalidated peers)")

            return {"status": "written", "key": key, "state": "M"}

    # ── Invalidate ────────────────────────────────────────────────────────────

    async def invalidate(self, key: str) -> dict:
        """
        Handle an INVALIDATE request from another node.
        Transitions local entry to INVALID state.
        """
        async with self._lock:
            entry = self._cache.get(key)
            if entry:
                old_state = entry.state
                if entry.state == MESIState.MODIFIED:
                    # Write back dirty data to backing store before invalidating
                    await self._write_back(key, entry.value)
                entry.state = MESIState.INVALID
                metrics.counter("cache_invalidations_total").increment()
                logger.debug(f"[{self.node_id}] INVALIDATE {key} ({old_state.value}→I)")
        return {"status": "invalidated", "key": key}

    # ── Peer Operations ───────────────────────────────────────────────────────

    async def _fetch_from_peers(self, key: str) -> Optional[dict]:
        """Ask peers if they have a valid copy of the key."""
        for peer in self.peers:
            resp = await self.mp.send(peer, f"/cache/peek/{key}", {}, method="GET")
            if resp and resp.get("value") is not None:
                return resp
        return None

    async def _broadcast_invalidate(self, key: str) -> None:
        """Send INVALIDATE to all peers for a key."""
        await self.mp.broadcast(self.peers, f"/cache/invalidate/{key}", {})
        logger.debug(f"[{self.node_id}] Broadcast INVALIDATE for {key}")

    async def _load_from_memory(self, key: str) -> Optional[Any]:
        """Simulate backing store read (returns None if key not found)."""
        return None

    async def _write_back(self, key: str, value: Any) -> None:
        """Simulate write-back to backing store for dirty M-state entries."""
        logger.debug(f"[{self.node_id}] Write-back {key} to memory")

    def peek(self, key: str) -> dict:
        """
        Return cached value without updating LRU order.
        Used by peers during fetch_from_peers.
        """
        entry = self._cache._store.get(key)
        if entry and entry.state not in (MESIState.INVALID,):
            return {"value": entry.value, "state": entry.state.value, "version": entry.version}
        return {"value": None}

    # ── Status & Metrics ──────────────────────────────────────────────────────

    def get_status(self) -> dict:
        snapshot = metrics.get_snapshot()
        hit_rate = metrics.cache_hit_rate()
        return {
            "node": self.node_id,
            "cache_size": len(self._cache),
            "capacity": config.CACHE_SIZE,
            "hit_rate": hit_rate,
            "keys": self._cache.all_keys()[:50],   # first 50
            "metrics": {
                "hits": snapshot["counters"].get("cache_hits_total", 0),
                "misses": snapshot["counters"].get("cache_misses_total", 0),
                "evictions": snapshot["counters"].get("cache_evictions_total", 0),
                "invalidations": snapshot["counters"].get("cache_invalidations_total", 0),
            },
            "raft": self.raft.get_status(),
        }
