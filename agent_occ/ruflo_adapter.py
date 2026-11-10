"""Ruflo (Claude Flow) Swarm Orchestration Adapter for agent-occ-lock.

Enables multi-player agent swarms running on Ruflo / Claude Flow to maintain
deterministic, zero-loss state synchronization under concurrent LLM latency.
Compatible with Ruflo's AgentDB and swarm memory protocols.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List, Optional, Tuple
from .engine import AgentOCCStore
from .models import TransactionResult, StateRecord, StateConflict


class RufloSwarmCoordinator:
    """Manages concurrent state synchronization for Ruflo multi-agent swarms.
    
    Prevents silent state loss when multiple Ruflo agents (e.g., Lead Scraper,
    Tech Stack Analyst, Code Reviewer) write to a shared session simultaneously.
    """

    def __init__(self, store: Optional[AgentOCCStore] = None):
        self.store = store or AgentOCCStore()

    def init_swarm_session(self, session_id: str, initial_state: Optional[Dict[str, Any]] = None, coordinator_id: str = "ruflo_coordinator") -> StateRecord:
        """Initialize a new multi-agent shared state session with revision 1."""
        return self.store.get_or_create(
            key=session_id,
            initial_data=initial_state or {},
            created_by=coordinator_id
        )

    def read_swarm_state(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Read current state and revision token for a swarm worker."""
        rec = self.store.read(session_id)
        if not rec:
            return None
        return {
            "session_id": rec.key,
            "revision": rec.version,
            "etag": rec.etag,
            "state": rec.data.copy(),
            "last_updated_by": rec.updated_by,
            "updated_at": rec.updated_at
        }

    def commit_agent_step(
        self,
        session_id: str,
        agent_id: str,
        expected_revision: int,
        delta: Dict[str, Any],
        auto_merge: bool = True
    ) -> TransactionResult:
        """Atomically commit a subagent's execution step with 3-way semantic diff merge.
        
        If a peer agent updated a disjoint key in the interim, the update is auto-merged
        without re-running the LLM. If an overlapping key collides, a StateConflict is raised.
        """
        # Ensure session exists
        if not self.store.read(session_id):
            self.init_swarm_session(session_id, coordinator_id=agent_id)

        return self.store.commit(
            key=session_id,
            expected_version=expected_revision,
            delta=delta,
            agent_id=agent_id,
            auto_merge=auto_merge
        )

    async def reconcile_parallel_swarm(
        self,
        session_id: str,
        agent_mutations: List[Dict[str, Any]],
        max_retries: int = 5
    ) -> Dict[str, Any]:
        """Reconcile multiple parallel subagents writing to shared session simultaneously.
        
        Each item in agent_mutations must be:
        { "agent_id": str, "delta": dict }
        """
        successes = []
        conflicts = []

        for mut in agent_mutations:
            agent_id = mut.get("agent_id", "unknown_worker")
            delta = mut.get("delta", {})

            # Attempt optimistic mutation with retry
            async def mutate_fn(current_data: Dict[str, Any]) -> Dict[str, Any]:
                return delta

            res = await self.store.mutate_async(
                key=session_id,
                mutate_fn=mutate_fn,
                agent_id=agent_id,
                max_retries=max_retries
            )

            if res.success:
                successes.append({"agent_id": agent_id, "revision": res.record.version if res.record else 0})
            else:
                conflicts.append({"agent_id": agent_id, "conflict": str(res.conflict)})

        final_rec = self.store.read(session_id)
        return {
            "session_id": session_id,
            "final_revision": final_rec.version if final_rec else 0,
            "final_state": final_rec.data if final_rec else {},
            "committed_agents": len(successes),
            "conflicts": conflicts
        }

    def export_ruflo_manifest(self, session_id: str) -> Dict[str, Any]:
        """Export session audit trail in Ruflo / AgentDB compatible schema."""
        rec = self.store.read(session_id)
        trail = self.store.get_audit_trail(session_id)
        active_agents = list(set(getattr(t, "agent_id", "system") for t in trail))
        serialized_trail = [
            {
                "key": getattr(t, "key", session_id),
                "from_version": getattr(t, "from_version", 0),
                "to_version": getattr(t, "to_version", 0),
                "agent_id": getattr(t, "agent_id", "system"),
                "diff": getattr(t, "diff", {}),
                "etag": getattr(t, "etag", ""),
                "timestamp": getattr(t, "timestamp", 0.0),
            }
            for t in trail
        ]
        return {
            "schema_version": "ruflo-occ-v1",
            "session_id": session_id,
            "current_revision": rec.version if rec else 0,
            "current_etag": rec.etag if rec else "",
            "active_agents": active_agents,
            "transaction_count": len(trail),
            "audit_trail": serialized_trail
        }
