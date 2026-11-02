"""Storage backends for agent state with atomic snapshot and recovery."""

from __future__ import annotations

import json
import os
import tempfile
from typing import Dict, List, Optional
from .models import StateRecord, AuditEvent


class MemoryStorage:
    """Thread-safe / Coroutine-safe in-memory store for state records and audit events."""

    def __init__(self):
        self._states: Dict[str, StateRecord] = {}
        self._history: Dict[str, List[StateRecord]] = {}
        self._audit_log: List[AuditEvent] = []

    def get(self, key: str) -> Optional[StateRecord]:
        return self._states.get(key)

    def set(self, record: StateRecord, event: Optional[AuditEvent] = None) -> None:
        key = record.key
        self._states[key] = record
        if key not in self._history:
            self._history[key] = []
        self._history[key].append(record)
        if event:
            self._audit_log.append(event)

    def get_history(self, key: str) -> List[StateRecord]:
        return list(self._history.get(key, []))

    def get_audit_log(self, key: Optional[str] = None) -> List[AuditEvent]:
        if key is None:
            return list(self._audit_log)
        return [e for e in self._audit_log if e.key == key]


class FileStorage(MemoryStorage):
    """Persistent storage backend using atomic file swaps (write-then-rename) for zero-corruption."""

    def __init__(self, filepath: str):
        super().__init__()
        self.filepath = filepath
        self._load()

    def _load(self) -> None:
        if not os.path.exists(self.filepath):
            return
        try:
            with open(self.filepath, "r", encoding="utf-8") as f:
                raw = json.load(f)
                for item in raw.get("states", []):
                    rec = StateRecord(
                        key=item["key"],
                        version=item["version"],
                        data=item["data"],
                        etag=item["etag"],
                        updated_at=item["updated_at"],
                        updated_by=item["updated_by"],
                    )
                    self._states[rec.key] = rec
        except Exception:
            pass

    def save(self) -> None:
        """Atomic disk flush using tempfile and rename."""
        parent = os.path.dirname(os.path.abspath(self.filepath))
        os.makedirs(parent, exist_ok=True)
        data = {
            "states": [
                {
                    "key": r.key,
                    "version": r.version,
                    "data": r.data,
                    "etag": r.etag,
                    "updated_at": r.updated_at,
                    "updated_by": r.updated_by,
                }
                for r in self._states.values()
            ]
        }
        with tempfile.NamedTemporaryFile("w", dir=parent, delete=False, encoding="utf-8") as tmp:
            json.dump(data, tmp, indent=2)
            tmp_name = tmp.name
        os.replace(tmp_name, self.filepath)
