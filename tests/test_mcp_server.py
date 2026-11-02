"""Tests for agent_occ MCP server integration."""

import json
import pytest
from agent_occ.mcp_server import read_agent_state, commit_agent_state_occ, store


def test_mcp_read_and_commit():
    # 1. Initialize state
    store.get_or_create(key="doc_shared", initial_data={"count": 0, "status": "init"}, created_by="agent_0")

    # 2. Read state via MCP tool
    read_res = json.loads(read_agent_state("doc_shared"))
    assert read_res["status"] == "success"
    assert read_res["version"] == 1
    assert read_res["data"]["count"] == 0

    # 3. Commit state via MCP tool
    commit_res = json.loads(commit_agent_state_occ(
        key="doc_shared",
        expected_version=1,
        delta={"count": 1, "agent_msg": "updated"},
        agent_id="agent_1"
    ))
    assert commit_res["status"] == "committed"
    assert commit_res["version"] == 2
    assert commit_res["data"]["count"] == 1

    # 4. Attempt stale commit (conflict detection)
    stale_res = json.loads(commit_agent_state_occ(
        key="doc_shared",
        expected_version=1, # Stale!
        delta={"count": 999}, # Collision on count
        agent_id="agent_2",
        auto_merge=True
    ))
    assert stale_res["status"] == "conflict_rejected"
    assert stale_res["expected_version"] == 1
    assert stale_res["actual_version"] == 2
