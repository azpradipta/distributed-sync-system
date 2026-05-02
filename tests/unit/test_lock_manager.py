"""
Unit Tests — Distributed Lock Manager
Tests lock acquisition, release, deadlock detection, and WFG cycle detection.
"""
import asyncio
import pytest
from collections import defaultdict
from unittest.mock import AsyncMock, MagicMock, patch
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from src.nodes.lock_manager import LockManager, LockState, LockRequest, LockType


@pytest.fixture
def lock_mgr():
    """LockManager with mocked infrastructure."""
    with patch("src.nodes.lock_manager.BaseNode.__init__", lambda self: None):
        mgr = LockManager.__new__(LockManager)
        mgr.node_id = "node1"
        mgr.peers = []
        mgr._locks = {}
        mgr._client_requests = {}
        mgr._wait_for = defaultdict(set)
        mgr._lock = asyncio.Lock()
        mgr._deadlock_task = None
        mgr.raft = MagicMock()
        mgr.raft.append_command = AsyncMock(return_value={"status": "committed", "index": 0})
        mgr.mp = MagicMock()
        return mgr


@pytest.mark.asyncio
async def test_acquire_exclusive_lock(lock_mgr):
    """Exclusive lock acquired when resource is free."""
    cmd = {
        "request_id": "r1", "lock_id": "res1",
        "client_id": "client1", "lock_type": "exclusive",
        "timeout": 30.0, "timestamp": 1000.0,
    }
    await lock_mgr._apply_acquire(cmd)
    ls = lock_mgr._locks["res1"]
    assert ls.exclusive_holder == "client1"
    assert ls.waiting_queue == []


@pytest.mark.asyncio
async def test_acquire_shared_lock_allows_multiple(lock_mgr):
    """Multiple clients can hold shared locks simultaneously."""
    for i, client in enumerate(["c1", "c2", "c3"]):
        cmd = {
            "request_id": f"r{i}", "lock_id": "res1",
            "client_id": client, "lock_type": "shared",
            "timeout": 30.0, "timestamp": float(i),
        }
        await lock_mgr._apply_acquire(cmd)
    ls = lock_mgr._locks["res1"]
    assert ls.exclusive_holder is None
    assert "c1" in ls.shared_holders
    assert "c2" in ls.shared_holders
    assert "c3" in ls.shared_holders


@pytest.mark.asyncio
async def test_exclusive_blocks_when_shared_held(lock_mgr):
    """Exclusive lock queued when shared holders exist."""
    await lock_mgr._apply_acquire({
        "request_id": "r1", "lock_id": "res1",
        "client_id": "reader", "lock_type": "shared",
        "timeout": 30.0, "timestamp": 1.0,
    })
    await lock_mgr._apply_acquire({
        "request_id": "r2", "lock_id": "res1",
        "client_id": "writer", "lock_type": "exclusive",
        "timeout": 30.0, "timestamp": 2.0,
    })
    ls = lock_mgr._locks["res1"]
    assert ls.exclusive_holder is None
    assert len(ls.waiting_queue) == 1
    assert ls.waiting_queue[0].client_id == "writer"


@pytest.mark.asyncio
async def test_release_grants_waiting_exclusive(lock_mgr):
    """Releasing shared lock grants waiting exclusive lock."""
    await lock_mgr._apply_acquire({
        "request_id": "r1", "lock_id": "res1",
        "client_id": "reader", "lock_type": "shared",
        "timeout": 30.0, "timestamp": 1.0,
    })
    await lock_mgr._apply_acquire({
        "request_id": "r2", "lock_id": "res1",
        "client_id": "writer", "lock_type": "exclusive",
        "timeout": 30.0, "timestamp": 2.0,
    })
    await lock_mgr._apply_release({"lock_id": "res1", "client_id": "reader"})
    ls = lock_mgr._locks["res1"]
    assert ls.exclusive_holder == "writer"
    assert ls.waiting_queue == []


@pytest.mark.asyncio
async def test_no_deadlock_detected_on_acyclic_wfg(lock_mgr):
    """Acyclic WFG: no deadlock reported."""
    lock_mgr._wait_for = {
        "A": {"B"},
        "B": {"C"},
    }
    cycles = lock_mgr._find_cycles()
    assert cycles == []


@pytest.mark.asyncio
async def test_deadlock_detected_on_cyclic_wfg(lock_mgr):
    """Cyclic WFG: deadlock detected."""
    lock_mgr._wait_for = {
        "A": {"B"},
        "B": {"C"},
        "C": {"A"},
    }
    cycles = lock_mgr._find_cycles()
    assert len(cycles) > 0
    # Cycle must include all three nodes
    cycle = cycles[0]
    clients_in_cycle = set(cycle)
    assert clients_in_cycle.issubset({"A", "B", "C"})


@pytest.mark.asyncio
async def test_two_node_deadlock(lock_mgr):
    """Simple 2-node mutual wait → deadlock."""
    lock_mgr._wait_for = {
        "X": {"Y"},
        "Y": {"X"},
    }
    cycles = lock_mgr._find_cycles()
    assert len(cycles) > 0


@pytest.mark.asyncio
async def test_release_nonexistent_lock(lock_mgr):
    """Releasing a non-existent lock does not raise."""
    # Should not throw
    await lock_mgr._apply_release({"lock_id": "nonexistent", "client_id": "c1"})


@pytest.mark.asyncio
async def test_acquire_via_api_calls_raft(lock_mgr):
    """Acquire method submits command via Raft."""
    result = await lock_mgr.acquire("res1", "client1", "exclusive")
    lock_mgr.raft.append_command.assert_called_once()
    assert result["status"] == "acquired"
    assert result["lock_id"] == "res1"


@pytest.mark.asyncio
async def test_acquire_returns_error_when_not_leader(lock_mgr):
    """Acquire returns error when node is not leader."""
    lock_mgr.raft.append_command = AsyncMock(return_value={
        "status": "not_leader", "leader": "http://node2:8002"
    })
    result = await lock_mgr.acquire("res1", "client1", "exclusive")
    assert result["status"] == "error"
    assert result["reason"] == "not_leader"
