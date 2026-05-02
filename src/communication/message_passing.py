"""
Message Passing Layer — Async HTTP-based inter-node communication.
Uses aiohttp for non-blocking RPC calls between cluster nodes.
"""
import asyncio
import logging
import time
from typing import Any, Dict, Optional

import aiohttp

from src.utils.metrics import metrics

logger = logging.getLogger(__name__)

# Timeout for individual RPC calls (seconds)
RPC_TIMEOUT = 0.5


class MessagePassing:
    """
    Provides async RPC primitives for inter-node communication.
    Each node creates one MessagePassing instance and reuses the session.
    """

    def __init__(self, node_id: str):
        self.node_id = node_id
        self._session: Optional[aiohttp.ClientSession] = None

    async def start(self) -> None:
        """Initialize the underlying HTTP session."""
        connector = aiohttp.TCPConnector(limit=100, ttl_dns_cache=300)
        timeout = aiohttp.ClientTimeout(total=RPC_TIMEOUT)
        self._session = aiohttp.ClientSession(connector=connector, timeout=timeout)
        logger.info(f"[{self.node_id}] MessagePassing started")

    async def stop(self) -> None:
        """Close the HTTP session gracefully."""
        if self._session and not self._session.closed:
            await self._session.close()
        logger.info(f"[{self.node_id}] MessagePassing stopped")

    async def send(
        self,
        peer_url: str,
        endpoint: str,
        payload: Dict[str, Any],
        method: str = "POST",
    ) -> Optional[Dict[str, Any]]:
        """
        Send an RPC message to a peer node.

        Args:
            peer_url:  Base URL of the target node, e.g. "http://node2:8002"
            endpoint:  Path, e.g. "/raft/vote"
            payload:   JSON-serialisable request body
            method:    HTTP method (default POST)

        Returns:
            Parsed JSON response, or None on failure.
        """
        if self._session is None or self._session.closed:
            logger.warning(f"[{self.node_id}] Session not started; dropping message")
            return None

        url = f"{peer_url}{endpoint}"
        start = time.perf_counter()

        try:
            metrics.counter("rpc_calls_total").increment()
            async with self._session.request(method, url, json=payload) as resp:
                elapsed = time.perf_counter() - start
                metrics.histogram("rpc_duration_seconds").observe(elapsed)
                if resp.status == 200:
                    return await resp.json()
                else:
                    body = await resp.text()
                    logger.warning(
                        f"[{self.node_id}] RPC {url} returned {resp.status}: {body[:200]}"
                    )
                    return None
        except asyncio.TimeoutError:
            metrics.counter("rpc_failures_total").increment()
            logger.debug(f"[{self.node_id}] RPC timeout → {url}")
            return None
        except aiohttp.ClientConnectorError:
            metrics.counter("rpc_failures_total").increment()
            logger.debug(f"[{self.node_id}] RPC connection refused → {url}")
            return None
        except Exception as exc:
            metrics.counter("rpc_failures_total").increment()
            logger.error(f"[{self.node_id}] RPC error → {url}: {exc}")
            return None

    async def broadcast(
        self,
        peers: list,
        endpoint: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Optional[Dict[str, Any]]]:
        """
        Send the same message to multiple peers concurrently.

        Returns:
            Dict mapping peer_url → response (or None on failure).
        """
        tasks = {peer: self.send(peer, endpoint, payload) for peer in peers}
        results = await asyncio.gather(*tasks.values(), return_exceptions=False)
        return dict(zip(tasks.keys(), results))
