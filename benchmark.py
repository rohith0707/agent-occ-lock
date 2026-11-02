"""Empirical Concurrency Benchmark: Naive State Overwriting vs. Agent-OCC-Lock.

Simulates 20 concurrent AI agents performing reads, simulated LLM reasoning,
and state mutations on a shared agent memory object.
"""

from __future__ import annotations

import asyncio
import time
from typing import Dict, Any
from agent_occ import AgentOCCStore


class NaiveSharedMemory:
    """Standard naive dictionary memory without concurrency control."""
    def __init__(self):
        self.state: Dict[str, Any] = {}

    def get_snapshot(self) -> Dict[str, Any]:
        return dict(self.state)

    def write_overwrite(self, snapshot: Dict[str, Any], delta: Dict[str, Any]):
        # Naive pattern: Worker merges delta into its cached snapshot and overwrites
        updated = dict(snapshot)
        updated.update(delta)
        self.state = updated


async def run_naive_benchmark(num_agents: int = 20, delay_ms: float = 20.0):
    naive = NaiveSharedMemory()
    start_time = time.perf_counter()

    async def worker(agent_idx: int):
        # 1. Read current state
        cached = naive.get_snapshot()
        # 2. Simulate LLM reasoning latency
        await asyncio.sleep(delay_ms / 1000.0)
        # 3. Naive overwrite
        naive.write_overwrite(cached, {f"agent_{agent_idx}_output": f"result_{agent_idx}"})

    tasks = [worker(i) for i in range(num_agents)]
    await asyncio.gather(*tasks)
    elapsed = time.perf_counter() - start_time

    retained_keys = len(naive.state)
    lost_updates = num_agents - retained_keys
    loss_rate = (lost_updates / num_agents) * 100

    return {
        "mode": "Naive In-Place Overwrites",
        "total_agents": num_agents,
        "retained_updates": retained_keys,
        "lost_updates": lost_updates,
        "loss_rate_pct": loss_rate,
        "elapsed_sec": round(elapsed, 4),
    }


async def run_occ_benchmark(num_agents: int = 20, delay_ms: float = 20.0):
    store = AgentOCCStore()
    store.get_or_create("shared_swarm_state", {})
    start_time = time.perf_counter()

    async def worker(agent_idx: int):
        async def mutate(data):
            # Simulate LLM reasoning latency
            await asyncio.sleep(delay_ms / 1000.0)
            return {f"agent_{agent_idx}_output": f"result_{agent_idx}"}

        res = await store.mutate_async(
            "shared_swarm_state",
            mutate,
            agent_id=f"agent_{agent_idx}",
            max_retries=10,
            backoff_base=0.02,
        )
        assert res.success, f"Agent {agent_idx} failed"

    tasks = [worker(i) for i in range(num_agents)]
    await asyncio.gather(*tasks)
    elapsed = time.perf_counter() - start_time

    final_rec = store.read("shared_swarm_state")
    retained_keys = len(final_rec.data)
    lost_updates = num_agents - retained_keys
    loss_rate = (lost_updates / num_agents) * 100

    return {
        "mode": "Agent-OCC-Lock (CAS + 3-Way Merge)",
        "total_agents": num_agents,
        "retained_updates": retained_keys,
        "lost_updates": lost_updates,
        "loss_rate_pct": loss_rate,
        "elapsed_sec": round(elapsed, 4),
    }


async def main():
    print("=" * 68)
    print("  MULTI-AGENT CONCURRENCY BENCHMARK (Simulated 20ms LLM Latency)")
    print("=" * 68)

    num_agents = 25
    delay_ms = 25.0

    print(f"\n[1/2] Running Naive Memory Overwrite Benchmark ({num_agents} concurrent agents)...")
    naive_res = await run_naive_benchmark(num_agents, delay_ms)
    print(f" -> Retained: {naive_res['retained_updates']}/{num_agents}")
    print(f" -> Lost Updates: {naive_res['lost_updates']} ({naive_res['loss_rate_pct']:.1f}% data loss!)")
    print(f" -> Elapsed: {naive_res['elapsed_sec']}s")

    print(f"\n[2/2] Running Agent-OCC-Lock Benchmark ({num_agents} concurrent agents)...")
    occ_res = await run_occ_benchmark(num_agents, delay_ms)
    print(f" -> Retained: {occ_res['retained_updates']}/{num_agents}")
    print(f" -> Lost Updates: {occ_res['lost_updates']} ({occ_res['loss_rate_pct']:.1f}% data loss)")
    print(f" -> Elapsed: {occ_res['elapsed_sec']}s")

    print("\n" + "=" * 68)
    print(" BENCHMARK SUMMARY TABLE")
    print("=" * 68)
    print(f"{'Metric':<30} | {'Naive Memory':<16} | {'Agent-OCC-Lock':<16}")
    print("-" * 68)
    print(f"{'Concurrent Workers':<30} | {naive_res['total_agents']:<16} | {occ_res['total_agents']:<16}")
    print(f"{'State Retained Updates':<30} | {naive_res['retained_updates']:<16} | {occ_res['retained_updates']:<16}")
    print(f"{'Lost Updates (Silent Loss)':<30} | {naive_res['lost_updates']:<16} | {occ_res['lost_updates']:<16}")
    print(f"{'Data Loss Rate (%)':<30} | {str(naive_res['loss_rate_pct'])+'%':<16} | {str(occ_res['loss_rate_pct'])+'%':<16}")
    print(f"{'Total Elapsed Time':<30} | {str(naive_res['elapsed_sec'])+'s':<16} | {str(occ_res['elapsed_sec'])+'s':<16}")
    print("=" * 68)


if __name__ == "__main__":
    asyncio.run(main())
