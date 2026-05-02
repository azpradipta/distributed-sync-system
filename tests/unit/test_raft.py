"""
Unit Tests — Raft Consensus Algorithm
Tests leader election, log replication, and term management.
"""
import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from src.consensus.raft import RaftNode, NodeState, LogEntry


@pytest.fixture
def make_node():
    """Factory for RaftNode with mocked message passing."""
    def _make(node_id="node1", peers=None):
        if peers is None:
            peers = ["http://node2:8002", "http://node3:8003"]
        node = RaftNode(node_id=node_id, peers=peers)
        node._mp = MagicMock()
        node._mp.start = AsyncMock()
        node._mp.stop = AsyncMock()
        node._mp.send = AsyncMock(return_value=None)
        node._mp.broadcast = AsyncMock(return_value={p: None for p in peers})
        return node
    return _make


@pytest.mark.asyncio
async def test_initial_state(make_node):
    """Node starts as FOLLOWER with term 0."""
    node = make_node()
    assert node.state == NodeState.FOLLOWER
    assert node.current_term == 0
    assert node.voted_for is None
    assert node.log == []


@pytest.mark.asyncio
async def test_handle_request_vote_grants_first_vote(make_node):
    """Node grants vote to first valid candidate."""
    node = make_node("node1")
    await node._mp.start()

    result = await node.handle_request_vote({
        "term": 1,
        "candidate_id": "node2",
        "last_log_index": -1,
        "last_log_term": 0,
    })
    assert result["vote_granted"] is True
    assert node.voted_for == "node2"
    assert node.current_term == 1


@pytest.mark.asyncio
async def test_handle_request_vote_rejects_lower_term(make_node):
    """Node rejects vote request with lower term."""
    node = make_node()
    node.current_term = 5

    result = await node.handle_request_vote({
        "term": 3,
        "candidate_id": "node2",
        "last_log_index": -1,
        "last_log_term": 0,
    })
    assert result["vote_granted"] is False


@pytest.mark.asyncio
async def test_handle_request_vote_rejects_second_candidate(make_node):
    """Node rejects second candidate in same term."""
    node = make_node()
    await node.handle_request_vote({
        "term": 1, "candidate_id": "node2",
        "last_log_index": -1, "last_log_term": 0,
    })
    result = await node.handle_request_vote({
        "term": 1, "candidate_id": "node3",
        "last_log_index": -1, "last_log_term": 0,
    })
    assert result["vote_granted"] is False


@pytest.mark.asyncio
async def test_step_down_on_higher_term(make_node):
    """Node steps down to Follower when seeing higher term."""
    node = make_node()
    node.state = NodeState.LEADER
    node.current_term = 3
    async with node._lock:
        await node._step_down(10)
    assert node.state == NodeState.FOLLOWER
    assert node.current_term == 10
    assert node.voted_for is None


@pytest.mark.asyncio
async def test_append_entries_heartbeat_accepted(make_node):
    """Empty AppendEntries (heartbeat) accepted by follower."""
    node = make_node()
    result = await node.handle_append_entries({
        "term": 1,
        "leader_id": "node2",
        "prev_log_index": -1,
        "prev_log_term": 0,
        "entries": [],
        "leader_commit": -1,
    })
    assert result["success"] is True
    assert node.current_leader == "node2"


@pytest.mark.asyncio
async def test_append_entries_log_replication(make_node):
    """AppendEntries correctly appends log entries."""
    node = make_node()
    entries = [{"term": 1, "index": 0, "command": {"type": "SET", "key": "x", "value": 1}}]
    result = await node.handle_append_entries({
        "term": 1,
        "leader_id": "node2",
        "prev_log_index": -1,
        "prev_log_term": 0,
        "entries": entries,
        "leader_commit": -1,
    })
    assert result["success"] is True
    assert len(node.log) == 1
    assert node.log[0].command["key"] == "x"


@pytest.mark.asyncio
async def test_append_entries_rejects_stale_term(make_node):
    """AppendEntries with lower term is rejected."""
    node = make_node()
    node.current_term = 5
    result = await node.handle_append_entries({
        "term": 3,
        "leader_id": "node2",
        "prev_log_index": -1,
        "prev_log_term": 0,
        "entries": [],
        "leader_commit": -1,
    })
    assert result["success"] is False


@pytest.mark.asyncio
async def test_become_leader_initialises_state(make_node):
    """Leader initialises nextIndex and matchIndex correctly."""
    node = make_node()
    node.current_term = 1
    node.state = NodeState.CANDIDATE
    async with node._lock:
        await node._become_leader()
    assert node.state == NodeState.LEADER
    for peer in node.peers:
        assert node.next_index[peer] == 0
        assert node.match_index[peer] == -1


@pytest.mark.asyncio
async def test_append_command_returns_not_leader_for_follower(make_node):
    """Non-leader nodes redirect client commands."""
    node = make_node()
    node.state = NodeState.FOLLOWER
    node.current_leader = "http://node2:8002"
    result = await node.append_command({"type": "SET", "key": "x", "value": 1})
    assert result["status"] == "not_leader"
    assert result["leader"] == "http://node2:8002"


def test_log_entry_serialisation():
    """LogEntry roundtrip serialisation."""
    entry = LogEntry(term=2, index=5, command={"type": "SET", "key": "k", "value": 42})
    d = entry.to_dict()
    restored = LogEntry.from_dict(d)
    assert restored.term == entry.term
    assert restored.index == entry.index
    assert restored.command == entry.command
