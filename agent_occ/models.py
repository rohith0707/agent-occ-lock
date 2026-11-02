"""Data models for Optimistic Concurrency Control (OCC) in multi-agent systems."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


def compute_state_hash(data: Dict[str, Any], version: int) -> str:
    """Compute deterministic SHA-256 fingerprint of state content + version."""
    payload = json.dumps({"version": version, "data": data}, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


@dataclass(frozen=True)
class StateRecord:
    """Immutable snapshot of a state entity at a specific revision."""
    key: str
    version: int
    data: Dict[str, Any]
    etag: str
    updated_at: float
    updated_by: str

    @classmethod
    def create(cls, key: str, data: Dict[str, Any], version: int = 1, updated_by: str = "system") -> StateRecord:
        now = time.time()
        etag = compute_state_hash(data, version)
        return cls(
            key=key,
            version=version,
            data=dict(data),
            etag=etag,
            updated_at=now,
            updated_by=updated_by,
        )


@dataclass
class AuditEvent:
    """Immutable ledger record of a successful state mutation."""
    key: str
    from_version: int
    to_version: int
    agent_id: str
    diff: Dict[str, Any]
    etag: str
    timestamp: float = field(default_factory=time.time)


@dataclass
class TransactionResult:
    """Outcome of an atomic optimistic commit attempt."""
    success: bool
    record: Optional[StateRecord] = None
    conflict: Optional[StateConflict] = None
    retries: int = 0
    auto_merged: bool = False


class StateConflict(Exception):
    """Raised when an agent attempts a CAS commit on stale state."""
    def __init__(
        self,
        key: str,
        expected_version: int,
        actual_version: int,
        conflicting_keys: List[str],
        agent_id: str,
    ):
        self.key = key
        self.expected_version = expected_version
        self.actual_version = actual_version
        self.conflicting_keys = conflicting_keys
        self.agent_id = agent_id
        super().__init__(
            f"CAS Conflict on key '{key}': Agent '{agent_id}' expected version {expected_version}, "
            f"but actual version is {actual_version}. Conflicting fields: {conflicting_keys}"
        )
