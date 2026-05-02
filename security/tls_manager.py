"""
TLS Manager & RBAC — Bonus D: Security & Encryption
====================================================
Provides:
  1. TLS certificate generation (self-signed, for demo)
  2. API key-based RBAC with role-permission mapping
  3. aiohttp request authentication middleware
"""
import hashlib
import logging
import os
import ssl
from datetime import datetime, timedelta
from typing import Callable, Dict, Optional, Set

from aiohttp import web

logger = logging.getLogger(__name__)

# ── RBAC ─────────────────────────────────────────────────────────────────────

# Permission sets per role
ROLES: Dict[str, Set[str]] = {
    "admin":    {"lock:read", "lock:write", "queue:read", "queue:write", "cache:read", "cache:write", "metrics:read", "audit:read"},
    "producer": {"queue:write"},
    "consumer": {"queue:read", "queue:write"},  # write needed for ack
    "reader":   {"cache:read", "lock:read", "metrics:read"},
}

# Hardcoded demo API keys (in production: load from secrets manager)
API_KEYS: Dict[str, str] = {
    "dev-secret-key-change-in-prod": "admin",
    "producer-key-123": "producer",
    "consumer-key-456": "consumer",
    "reader-key-789": "reader",
}

ENDPOINT_PERMISSIONS: Dict[str, str] = {
    "POST /lock/acquire":  "lock:write",
    "POST /lock/release":  "lock:write",
    "GET /lock/status":    "lock:read",
    "POST /queue/produce": "queue:write",
    "POST /queue/consume": "queue:read",
    "POST /queue/ack":     "queue:write",
    "GET /queue/status":   "queue:read",
    "GET /cache":          "cache:read",
    "PUT /cache":          "cache:write",
    "DELETE /cache":       "cache:write",
    "GET /metrics":        "metrics:read",
    "GET /audit":          "audit:read",
}


class TLSManager:
    """Manages TLS certificates for inter-node encryption."""

    def __init__(self, cert_file: str, key_file: str):
        self.cert_file = cert_file
        self.key_file = key_file
        os.makedirs(os.path.dirname(cert_file), exist_ok=True)

    def generate_self_signed(self, node_id: str) -> None:
        """Generate a self-signed certificate for demo purposes."""
        try:
            from cryptography import x509
            from cryptography.x509.oid import NameOID
            from cryptography.hazmat.primitives import hashes, serialization
            from cryptography.hazmat.primitives.asymmetric import rsa
            import ipaddress

            key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
            subject = issuer = x509.Name([
                x509.NameAttribute(NameOID.COMMON_NAME, node_id),
                x509.NameAttribute(NameOID.ORGANIZATION_NAME, "DistributedSyncSystem"),
            ])
            cert = (
                x509.CertificateBuilder()
                .subject_name(subject)
                .issuer_name(issuer)
                .public_key(key.public_key())
                .serial_number(x509.random_serial_number())
                .not_valid_before(datetime.utcnow())
                .not_valid_after(datetime.utcnow() + timedelta(days=365))
                .add_extension(
                    x509.SubjectAlternativeName([
                        x509.DNSName("localhost"),
                        x509.DNSName(node_id),
                        x509.IPAddress(ipaddress.IPv4Address("127.0.0.1")),
                    ]),
                    critical=False,
                )
                .sign(key, hashes.SHA256())
            )
            with open(self.cert_file, "wb") as f:
                f.write(cert.public_bytes(serialization.Encoding.PEM))
            with open(self.key_file, "wb") as f:
                f.write(key.private_bytes(
                    serialization.Encoding.PEM,
                    serialization.PrivateFormat.TraditionalOpenSSL,
                    serialization.NoEncryption(),
                ))
            logger.info(f"Generated self-signed cert for {node_id}: {self.cert_file}")
        except ImportError:
            logger.warning("cryptography library not installed. TLS disabled.")

    def get_ssl_context(self) -> Optional[ssl.SSLContext]:
        if not os.path.exists(self.cert_file) or not os.path.exists(self.key_file):
            return None
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(self.cert_file, self.key_file)
        return ctx


# ── RBAC Middleware ───────────────────────────────────────────────────────────

def get_role(api_key: str) -> Optional[str]:
    return API_KEYS.get(api_key)


def has_permission(role: str, permission: str) -> bool:
    return permission in ROLES.get(role, set())


@web.middleware
async def auth_middleware(request: web.Request, handler: Callable) -> web.Response:
    """
    aiohttp middleware: validates X-API-Key header and enforces RBAC.
    Internal Raft RPCs (/raft/*) bypass authentication.
    Health and metrics endpoints are open by default for demo.
    """
    path = request.path
    method = request.method

    # Bypass auth for internal RPCs and health check
    if (
        path.startswith("/raft/") or 
        path.startswith("/cache/peek/") or 
        path.startswith("/cache/invalidate/") or 
        path == "/queue/store" or 
        path == "/health"
    ):
        return await handler(request)

    api_key = request.headers.get("X-API-Key", "")
    role = get_role(api_key)

    if not role:
        raise web.HTTPUnauthorized(
            reason="Missing or invalid X-API-Key",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    # Check permission for this endpoint
    perm_key = f"{method} {path.rstrip('/')}"
    for pattern, perm in ENDPOINT_PERMISSIONS.items():
        pat_method, pat_path = pattern.split(" ", 1)
        if method == pat_method and path.startswith(pat_path):
            if not has_permission(role, perm):
                raise web.HTTPForbidden(
                    reason=f"Role '{role}' lacks permission '{perm}'"
                )
            break

    # Attach role to request for downstream use
    request["role"] = role
    request["client_id"] = api_key[:16]  # use first 16 chars as client_id
    return await handler(request)
