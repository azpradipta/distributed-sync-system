"""
Configuration management for Distributed Sync System.
Loads settings from environment variables (.env file supported).
"""
import os
import sys
from typing import List
from dotenv import load_dotenv

load_dotenv()


class Config:
    """Centralized configuration loaded from environment variables."""

    # ── Node Identity ───────────────────────────────────────────────────────
    NODE_ID: str = os.getenv("NODE_ID", "node1")
    NODE_HOST: str = os.getenv("NODE_HOST", "0.0.0.0")
    NODE_PORT: int = int(os.getenv("NODE_PORT", "8001"))

    # ── Cluster ─────────────────────────────────────────────────────────────
    _peer_env: str = os.getenv("PEER_NODES", "")
    PEER_NODES: List[str] = [p.strip() for p in _peer_env.split(",") if p.strip()]

    # ── Redis ────────────────────────────────────────────────────────────────
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB: int = int(os.getenv("REDIS_DB", "0"))
    REDIS_PASSWORD: str = os.getenv("REDIS_PASSWORD", "")

    # ── Raft Timing (seconds) ────────────────────────────────────────────────
    ELECTION_TIMEOUT_MIN: float = float(os.getenv("ELECTION_TIMEOUT_MIN", "1.5"))
    ELECTION_TIMEOUT_MAX: float = float(os.getenv("ELECTION_TIMEOUT_MAX", "3.0"))
    HEARTBEAT_INTERVAL: float = float(os.getenv("HEARTBEAT_INTERVAL", "0.5"))

    # ── Distributed Lock ─────────────────────────────────────────────────────
    LOCK_TIMEOUT: float = float(os.getenv("LOCK_TIMEOUT", "30.0"))
    DEADLOCK_CHECK_INTERVAL: float = float(os.getenv("DEADLOCK_CHECK_INTERVAL", "5.0"))

    # ── Distributed Queue ────────────────────────────────────────────────────
    QUEUE_MAX_SIZE: int = int(os.getenv("QUEUE_MAX_SIZE", "10000"))
    MESSAGE_RETRY_INTERVAL: float = float(os.getenv("MESSAGE_RETRY_INTERVAL", "5.0"))
    MESSAGE_MAX_RETRIES: int = int(os.getenv("MESSAGE_MAX_RETRIES", "3"))
    VIRTUAL_NODES: int = int(os.getenv("VIRTUAL_NODES", "150"))

    # ── Cache ─────────────────────────────────────────────────────────────────
    CACHE_SIZE: int = int(os.getenv("CACHE_SIZE", "1000"))

    # ── Security (Bonus D) ───────────────────────────────────────────────────
    TLS_ENABLED: bool = os.getenv("TLS_ENABLED", "false").lower() == "true"
    TLS_CERT_FILE: str = os.getenv("TLS_CERT_FILE", "security/certs/node.crt")
    TLS_KEY_FILE: str = os.getenv("TLS_KEY_FILE", "security/certs/node.key")
    AUDIT_LOG_FILE: str = os.getenv("AUDIT_LOG_FILE", "logs/audit.log")
    API_KEY: str = os.getenv("API_KEY", "dev-secret-key-change-in-prod")

    # ── Logging ──────────────────────────────────────────────────────────────
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    @classmethod
    def get_redis_url(cls) -> str:
        if cls.REDIS_PASSWORD:
            return f"redis://:{cls.REDIS_PASSWORD}@{cls.REDIS_HOST}:{cls.REDIS_PORT}/{cls.REDIS_DB}"
        return f"redis://{cls.REDIS_HOST}:{cls.REDIS_PORT}/{cls.REDIS_DB}"

    @classmethod
    def node_url(cls) -> str:
        """Self URL (used by peers to reach this node)."""
        return f"http://{cls.NODE_ID}:{cls.NODE_PORT}"

    @classmethod
    def all_nodes(cls) -> List[str]:
        """All nodes including self."""
        return [cls.node_url()] + cls.PEER_NODES


config = Config()
