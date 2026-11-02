"""Model Context Protocol (MCP) server for agent-occ-lock.
Exposes concurrency-safe distributed agent state tools over stdio/HTTP.
"""

from __future__ import annotations

import json
from typing import Dict, Any, Optional
from mcp.server.mcpserver import MCPServer
from .engine import AgentOCCStore
from .models import StateRecord, StateConflict

server = MCPServer("agent-occ-lock")
store = AgentOCCStore()


@server.tool(name="read_agent_state", description="Read a document state with its current concurrency version token.")
def read_agent_state(key: str) -> str:
    """Fetch current state record and version."""
    rec = store.read(key)
    if not rec:
        return json.dumps({"status": "not_found", "key": key})
    return json.dumps({
        "status": "success",
        "key": rec.key,
        "version": rec.version,
        "data": rec.data,
        "updated_by": rec.updated_by,
        "etag": rec.etag,
        "updated_at": rec.updated_at,
    }, indent=2)


@server.tool(name="commit_agent_state_occ", description="Commit state updates with optimistic lock validation and automatic 3-way delta reconciliation.")
def commit_agent_state_occ(key: str, expected_version: int, delta: Dict[str, Any], agent_id: str, auto_merge: bool = True) -> str:
    """Perform atomic OCC commit."""
    # Ensure key exists
    if not store.read(key):
        store.get_or_create(key=key, initial_data={}, created_by=agent_id)

    res = store.commit(
        key=key,
        expected_version=expected_version,
        delta=delta,
        agent_id=agent_id,
        auto_merge=auto_merge
    )
    if res.success and res.record:
        return json.dumps({
            "status": "committed",
            "key": res.record.key,
            "version": res.record.version,
            "data": res.record.data,
            "updated_by": res.record.updated_by
        }, indent=2)
    else:
        conflict = res.conflict
        return json.dumps({
            "status": "conflict_rejected",
            "key": conflict.key if conflict else key,
            "expected_version": conflict.expected_version if conflict else expected_version,
            "actual_version": conflict.actual_version if conflict else None,
            "conflicting_fields": conflict.conflicting_keys if conflict else []
        }, indent=2)


def main():
    """Start MCP server on stdio transport."""
    server.run(transport="stdio")


if __name__ == "__main__":
    main()
