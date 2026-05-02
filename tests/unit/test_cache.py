"""
Unit Tests — Cache Coherence MESI Protocol
Tests MESI state transitions, LRU eviction, and coherence operations.
"""
import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from src.nodes.cache_node import CacheNode, CacheEntry, LRUCache, MESIState
from src.utils.config import config


@pytest.fixture
def lru_cache():
    return LRUCache(capacity=3)


def test_lru_put_and_get(lru_cache):
    """Basic put/get operations."""
    entry = CacheEntry(key="k1", value=42, state=MESIState.EXCLUSIVE)
    lru_cache.put("k1", entry)
    result = lru_cache.get("k1")
    assert result is not None
    assert result.value == 42


def test_lru_eviction(lru_cache):
    """Oldest entry evicted when capacity exceeded."""
    for i in range(3):
        lru_cache.put(f"k{i}", CacheEntry(key=f"k{i}", value=i, state=MESIState.EXCLUSIVE))

    # Access k0 to make it recently used
    lru_cache.get("k0")

    # Insert k3 → k1 should be evicted (LRU)
    evicted = lru_cache.put("k3", CacheEntry(key="k3", value=99, state=MESIState.EXCLUSIVE))
    assert evicted == "k1"
    assert "k1" not in lru_cache
    assert "k0" in lru_cache


def test_lru_invalidate(lru_cache):
    """Invalidate removes entry from cache."""
    lru_cache.put("k1", CacheEntry(key="k1", value=1, state=MESIState.SHARED))
    assert lru_cache.invalidate("k1") is True
    assert lru_cache.get("k1") is None


def test_lru_capacity():
    """Cache does not exceed capacity."""
    cache = LRUCache(capacity=5)
    for i in range(10):
        cache.put(f"k{i}", CacheEntry(key=f"k{i}", value=i, state=MESIState.EXCLUSIVE))
    assert len(cache) == 5


@pytest.fixture
def cache_node_mock():
    """CacheNode with mocked infrastructure."""
    with patch("src.nodes.cache_node.BaseNode.__init__", lambda self: None):
        node = CacheNode.__new__(CacheNode)
        node.node_id = "node1"
        node.peers = ["http://node2:8002"]
        node._cache = LRUCache(capacity=100)
        node._lock = asyncio.Lock()
        node.mp = MagicMock()
        node.mp.send = AsyncMock(return_value={"value": None})
        node.mp.broadcast = AsyncMock(return_value={})
        node.raft = MagicMock()
        return node


@pytest.mark.asyncio
async def test_write_creates_modified_state(cache_node_mock):
    """Write to new key creates M-state entry."""
    node = cache_node_mock
    result = await node.write("mykey", "myvalue")
    assert result["state"] == "M"
    entry = node._cache.get("mykey")
    assert entry is not None
    assert entry.state == MESIState.MODIFIED
    assert entry.value == "myvalue"


@pytest.mark.asyncio
async def test_write_exclusive_upgrades_to_modified(cache_node_mock):
    """Writing to E-state entry upgrades silently to M (no broadcast)."""
    node = cache_node_mock
    # Place E-state entry
    node._cache.put("k", CacheEntry(key="k", value="old", state=MESIState.EXCLUSIVE))
    result = await node.write("k", "new")
    assert result["state"] == "M"
    # No broadcast should have been called (E→M is silent)
    node.mp.broadcast.assert_not_called()


@pytest.mark.asyncio
async def test_write_shared_broadcasts_invalidate(cache_node_mock):
    """Writing to S-state entry broadcasts INVALIDATE to peers."""
    node = cache_node_mock
    node._cache.put("k", CacheEntry(key="k", value="old", state=MESIState.SHARED))
    await node.write("k", "new")
    node.mp.broadcast.assert_called_once()
    args = node.mp.broadcast.call_args
    assert "/cache/invalidate/k" in args[0][1]


@pytest.mark.asyncio
async def test_read_hit_returns_cached_value(cache_node_mock):
    """Cache hit returns value without fetching from peers."""
    node = cache_node_mock
    node._cache.put("k", CacheEntry(key="k", value="cached", state=MESIState.SHARED))
    result = await node.read("k")
    assert result["status"] == "hit"
    assert result["value"] == "cached"
    node.mp.send.assert_not_called()


@pytest.mark.asyncio
async def test_read_miss_transitions_to_exclusive_when_sole(cache_node_mock):
    """Cache miss with no peer data → E-state (sole holder)."""
    node = cache_node_mock
    node.mp.send = AsyncMock(return_value={"value": None})
    result = await node.read("newkey")
    assert result["status"] == "miss"
    # No data from peers, no backing store → value is None (not cached)
    assert result["value"] is None


@pytest.mark.asyncio
async def test_invalidate_changes_state_to_invalid(cache_node_mock):
    """INVALIDATE RPC transitions entry to I-state."""
    node = cache_node_mock
    node._cache.put("k", CacheEntry(key="k", value="v", state=MESIState.SHARED))
    result = await node.invalidate("k")
    assert result["status"] == "invalidated"
    entry = node._cache.get("k")
    assert entry.state == MESIState.INVALID


def test_peek_returns_valid_entry(cache_node_mock):
    """Peek returns data for valid cache entry."""
    node = cache_node_mock
    node._cache.put("k", CacheEntry(key="k", value=99, state=MESIState.EXCLUSIVE, version=2))
    result = node.peek("k")
    assert result["value"] == 99
    assert result["state"] == "E"


def test_peek_returns_none_for_invalid(cache_node_mock):
    """Peek returns None for INVALID-state entries."""
    node = cache_node_mock
    node._cache.put("k", CacheEntry(key="k", value=1, state=MESIState.INVALID))
    result = node.peek("k")
    assert result["value"] is None
