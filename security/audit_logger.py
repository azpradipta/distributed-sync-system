"""
Audit Logger — Bonus D: Security & Encryption
==============================================
Tamper-evident audit log using hash chaining.
Each log entry contains a SHA-256 hash of the previous entry,
creating an immutable chain detectable if records are altered.
"""
import asyncio
import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass, asdict
from typing import List, Optional

logger = logging.getLogger(__name__)


@dataclass
class AuditEntry:
    seq: int
    timestamp: float
    node_id: str
    client_id: str
    action: str
    resource: str
    status: str
    details: dict
    prev_hash: str
    entry_hash: str = ""

    def compute_hash(self) -> str:
        content = json.dumps({
            "seq": self.seq,
            "timestamp": self.timestamp,
            "node_id": self.node_id,
            "client_id": self.client_id,
            "action": self.action,
            "resource": self.resource,
            "status": self.status,
            "details": self.details,
            "prev_hash": self.prev_hash,
        }, sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()


class AuditLogger:
    """
    Tamper-evident audit logger with hash chaining.
    Each entry's hash depends on the previous entry,
    making retroactive modification detectable.
    """

    def __init__(self, node_id: str, log_file: str = "logs/audit.log"):
        self.node_id = node_id
        self.log_file = log_file
        self._seq: int = 0
        self._last_hash: str = "genesis"
        self._lock = asyncio.Lock()
        os.makedirs(os.path.dirname(log_file), exist_ok=True)

    async def log(
        self,
        client_id: str,
        action: str,
        resource: str,
        status: str = "success",
        details: dict = None,
    ) -> AuditEntry:
        """Record an audit event."""
        async with self._lock:
            self._seq += 1
            entry = AuditEntry(
                seq=self._seq,
                timestamp=time.time(),
                node_id=self.node_id,
                client_id=client_id,
                action=action,
                resource=resource,
                status=status,
                details=details or {},
                prev_hash=self._last_hash,
            )
            entry.entry_hash = entry.compute_hash()
            self._last_hash = entry.entry_hash

            line = json.dumps(asdict(entry))
            with open(self.log_file, "a") as f:
                f.write(line + "\n")

            logger.info(f"AUDIT [{self.node_id}] {action} {resource} by {client_id} → {status}")
            return entry

    def verify_integrity(self) -> dict:
        """
        Verify chain integrity by re-computing hashes.
        Returns: {"valid": bool, "tampered_at": Optional[int]}
        """
        if not os.path.exists(self.log_file):
            return {"valid": True, "entries": 0}

        prev_hash = "genesis"
        with open(self.log_file) as f:
            for line in f:
                entry_dict = json.loads(line.strip())
                entry = AuditEntry(**entry_dict)
                expected_hash = entry.compute_hash()
                if entry.entry_hash != expected_hash:
                    return {"valid": False, "tampered_at": entry.seq}
                if entry.prev_hash != prev_hash:
                    return {"valid": False, "tampered_at": entry.seq}
                prev_hash = entry.entry_hash

        return {"valid": True, "entries": self._seq}

    def get_recent(self, n: int = 50) -> List[dict]:
        """Return last N audit entries."""
        if not os.path.exists(self.log_file):
            return []
        with open(self.log_file) as f:
            lines = f.readlines()
        return [json.loads(l) for l in lines[-n:]]
