"""Comprehensive test suite for AgentOCCStore concurrency, 3-way merging, and serializability."""

import asyncio
import pytest
from agent_occ import AgentOCCStore, StateConflict


def test_basic_lifecycle():
    store = AgentOCCStore()
    record = store.get_or_create("task_123", {"status": "pending"})
    assert record.version == 1
    assert record.data == {"status": "pending"}

    # Clean commit
    res = store.commit("task_123", expected_version=1, delta={"status": "in_progress"}, agent_id="worker_1")
    assert res.success is True
    assert res.record.version == 2
    assert res.record.data["status"] == "in_progress"
    assert res.record.updated_by == "worker_1"


def test_stale_commit_rejection_without_merge():
    store = AgentOCCStore()
    store.get_or_create("order_99", {"step": 1})

    # Worker A advances state to v2
    store.commit("order_99", expected_version=1, delta={"step": 2}, agent_id="agent_a")

    # Worker B tries to commit based on stale v1 with auto_merge disabled
    res = store.commit("order_99", expected_version=1, delta={"step": 3}, agent_id="agent_b", auto_merge=False)
    assert res.success is False
    assert isinstance(res.conflict, StateConflict)
    assert res.conflict.expected_version == 1
    assert res.conflict.actual_version == 2


def test_three_way_auto_merge_disjoint_fields():
    store = AgentOCCStore()
    store.get_or_create("shared_doc", {"summary": "initial", "tags": []})

    # Agent 1 updates summary (v1 -> v2)
    res1 = store.commit("shared_doc", expected_version=1, delta={"summary": "Agent 1 summary"}, agent_id="agent_1")
    assert res1.success is True
    assert res1.record.version == 2

    # Agent 2 started at v1 and updates tags (disjoint key). Should auto-merge cleanly!
    res2 = store.commit("shared_doc", expected_version=1, delta={"tags": ["ai", "eval"]}, agent_id="agent_2", auto_merge=True)
    assert res2.success is True
    assert res2.auto_merged is True
    assert res2.record.version == 3
    # Both changes must be intact!
    assert res2.record.data["summary"] == "Agent 1 summary"
    assert res2.record.data["tags"] == ["ai", "eval"]


def test_three_way_collision_detection():
    store = AgentOCCStore()
    store.get_or_create("budget", {"amount": 100})

    # Agent A updates amount to 150 (v1 -> v2)
    store.commit("budget", expected_version=1, delta={"amount": 150}, agent_id="agent_a")

    # Agent B also tries to update amount to 200 based on stale v1
    res = store.commit("budget", expected_version=1, delta={"amount": 200}, agent_id="agent_b", auto_merge=True)
    assert res.success is False
    assert res.conflict is not None


@pytest.mark.asyncio
async def test_concurrent_agent_swarm_zero_lost_updates():
    """Simulate 10 distributed agents concurrently updating distinct metrics on a shared state.

    Without OCC, naive overwriting loses ~60% of agent updates.
    With AgentOCCStore, all 10 updates must be preserved (0% loss).
    """
    store = AgentOCCStore()
    store.get_or_create("metrics_aggregator", {})

    agent_count = 10

    async def worker(agent_idx: int):
        agent_id = f"worker_{agent_idx}"
        async def mutate(data):
            # Simulate variable LLM inference latency (10-30ms)
            await asyncio.sleep(0.01 + (agent_idx * 0.002))
            return {f"metric_{agent_idx}": agent_idx * 100}

        result = await store.mutate_async("metrics_aggregator", mutate, agent_id=agent_id, max_retries=10)
        assert result.success is True, f"{agent_id} failed to commit"

    # Run all 10 agents concurrently
    tasks = [worker(i) for i in range(agent_count)]
    await asyncio.gather(*tasks)

    # Verify final state has every single agent's contribution
    final_record = store.read("metrics_aggregator")
    assert final_record is not None
    assert len(final_record.data) == agent_count
    for i in range(agent_count):
        assert final_record.data[f"metric_{i}"] == i * 100

    # Verify audit log recorded all transactions
    trail = store.get_audit_trail("metrics_aggregator")
    assert len(trail) == agent_count
