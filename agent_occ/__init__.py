"""agent-occ-lock: Optimistic Concurrency Control (OCC) for Distributed Multi-Agent Systems."""

from .models import StateRecord, StateConflict, TransactionResult, AuditEvent, compute_state_hash
from .storage import MemoryStorage, FileStorage
from .engine import AgentOCCStore

__version__ = "0.1.0"
__all__ = [
    "AgentOCCStore",
    "StateRecord",
    "StateConflict",
    "TransactionResult",
    "AuditEvent",
    "MemoryStorage",
    "FileStorage",
    "compute_state_hash",
]
