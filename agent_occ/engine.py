"""Core Optimistic Concurrency Control (OCC) engine with 3-way delta reconciliation."""

from __future__ import annotations

import asyncio
import random
import threading
import time
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set, Tuple
from .models import StateRecord, StateConflict, TransactionResult, AuditEvent
from .storage import MemoryStorage


class AgentOCCStore:
    """Distributed Optimistic Concurrency Control store for autonomous agents.

    Eliminates lost updates during concurrent multi-agent executions without
    requiring pessimistic locks or distributed deadlocks.
    """

    def __init__(self, storage: Optional[MemoryStorage] = None):
        self._storage = storage or MemoryStorage()
        self._lock = threading.Lock()
        self._async_lock = asyncio.Lock()

    def get_or_create(self, key: str, initial_data: Optional[Dict[str, Any]] = None, created_by: str = "system") -> StateRecord:
        """Fetch existing record or initialize a new version 1 record."""
        with self._lock:
            rec = self._storage.get(key)
            if rec is None:
                rec = StateRecord.create(key=key, data=initial_data or {}, version=1, updated_by=created_by)
                self._storage.set(rec)
            return rec

    def read(self, key: str) -> Optional[StateRecord]:
        """Read the latest state snapshot and its revision token."""
        return self._storage.get(key)

    def _resolve_3way_merge(
        self,
        base_record: Optional[StateRecord],
        current_record: StateRecord,
        incoming_delta: Dict[str, Any],
    ) -> Tuple[bool, Dict[str, Any], List[str]]:
        """Perform 3-way semantic diff between Base (expected), Current, and Incoming Delta.

        Returns:
            (can_merge, merged_data, conflicting_keys)
        """
        if base_record is None:
            # Cannot safely 3-way merge without base history; reject on conflict
            return False, {}, list(incoming_delta.keys())

        # Keys changed between base_version and current_version by other agents
        changed_by_others: Set[str] = set()
        for k, v in current_record.data.items():
            if k not in base_record.data or base_record.data[k] != v:
                changed_by_others.add(k)
        for k in base_record.data:
            if k not in current_record.data:
                changed_by_others.add(k)

        # Keys that the incoming delta intends to modify
        changed_by_agent = set(incoming_delta.keys())

        # Check for direct collisions
        conflicts = list(changed_by_others.intersection(changed_by_agent))
        if conflicts:
            return False, {}, conflicts

        # Clean orthogonal changes: merge incoming delta on top of latest current state
        merged = dict(current_record.data)
        merged.update(incoming_delta)
        return True, merged, []

    def commit(
        self,
        key: str,
        expected_version: int,
        delta: Dict[str, Any],
        agent_id: str,
        auto_merge: bool = True,
    ) -> TransactionResult:
        """Attempt an atomic CAS commit.

        Args:
            key: State identifier.
            expected_version: The version number read when the agent began its reasoning.
            delta: Dictionary of updated/added keys to merge into state.
            agent_id: Unique caller identifier for audit logging.
            auto_merge: If True, resolves non-overlapping field changes automatically.

        Returns:
            TransactionResult with updated StateRecord or StateConflict.
        """
        with self._lock:
            current = self._storage.get(key)
            if current is None:
                raise KeyError(f"State key '{key}' does not exist.")

            # Happy path: No intervening updates
            if current.version == expected_version:
                new_data = dict(current.data)
                new_data.update(delta)
                new_record = StateRecord.create(
                    key=key,
                    data=new_data,
                    version=current.version + 1,
                    updated_by=agent_id,
                )
                event = AuditEvent(
                    key=key,
                    from_version=current.version,
                    to_version=new_record.version,
                    agent_id=agent_id,
                    diff=delta,
                    etag=new_record.etag,
                )
                self._storage.set(new_record, event)
                return TransactionResult(success=True, record=new_record, auto_merged=False)

            # Optimistic Conflict Detected: State advanced while agent was thinking
            if auto_merge:
                # Find base record from history
                history = self._storage.get_history(key)
                base = next((r for r in history if r.version == expected_version), None)
                can_merge, merged_data, conflicts = self._resolve_3way_merge(base, current, delta)

                if can_merge:
                    new_record = StateRecord.create(
                        key=key,
                        data=merged_data,
                        version=current.version + 1,
                        updated_by=agent_id,
                    )
                    event = AuditEvent(
                        key=key,
                        from_version=current.version,
                        to_version=new_record.version,
                        agent_id=agent_id,
                        diff=delta,
                        etag=new_record.etag,
                    )
                    self._storage.set(new_record, event)
                    return TransactionResult(success=True, record=new_record, auto_merged=True)

            # Unresolvable conflict
            conflict = StateConflict(
                key=key,
                expected_version=expected_version,
                actual_version=current.version,
                conflicting_keys=list(delta.keys()),
                agent_id=agent_id,
            )
            return TransactionResult(success=False, conflict=conflict)

    async def mutate_async(
        self,
        key: str,
        mutate_fn: Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]],
        agent_id: str,
        max_retries: int = 5,
        backoff_base: float = 0.05,
        auto_merge: bool = True,
    ) -> TransactionResult:
        """Run an async state mutation with automatic OCC retry and exponential backoff.

        Ideal for wrapping asynchronous LLM calls and tool executions.
        """
        retries = 0
        while retries <= max_retries:
            snapshot = self.get_or_create(key)
            expected_ver = snapshot.version

            # Agent performs its slow reasoning / tool calls using snapshot data
            delta = await mutate_fn(dict(snapshot.data))

            # Attempt atomic commit
            result = self.commit(
                key=key,
                expected_version=expected_ver,
                delta=delta,
                agent_id=agent_id,
                auto_merge=auto_merge,
            )
            if result.success:
                result.retries = retries
                return result

            # Conflict occurred; calculate backoff with jitter
            retries += 1
            if retries > max_retries:
                return TransactionResult(success=False, conflict=result.conflict, retries=retries)

            sleep_duration = backoff_base * (2 ** (retries - 1)) + random.uniform(0.01, 0.05)
            await asyncio.sleep(sleep_duration)

        return TransactionResult(success=False, retries=retries)

    def get_audit_trail(self, key: Optional[str] = None) -> List[AuditEvent]:
        """Retrieve complete audit ledger of all atomic revisions."""
        return self._storage.get_audit_log(key)
