# agent-occ-lock

[![CI Tests](https://img.shields.io/badge/tests-5%20passed-brightgreen.svg)]()
[![Zero Dependencies](https://img.shields.io/badge/dependencies-zero-blue.svg)]()
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)]()
[![AgentControl Commercial](https://img.shields.io/badge/Commercial%20License-AgentControl%20($149)-00C853.svg)](https://balsarohith.gumroad.com/l/vnztsy)
[![LeadHunter Pro](https://img.shields.io/badge/LeadHunter%20Pro-5,000%20Leads%20($97)-FF90E8.svg)](https://balsarohith.gumroad.com/l/LeadHunter)
[![Whop Store](https://img.shields.io/badge/Whop-Verified%20Store-blue.svg)](https://whop.com/checkout/plan_BHAobTHbElFCx/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Optimistic Concurrency Control (OCC) and 3-way delta state resolution for distributed multi-agent LLM systems.

> [!NOTE]
> **📺 90-Second Architecture Explainer Video Walkthrough**  
> We released a full 1080p video teardown explaining compounding token spikes and multi-agent state concurrency.  
> ▶️ **[Watch / Download the Explainer Video (MP4)](https://github.com/rohith0707/llm-cost-watch/releases/download/v1.0.0/llm_cost_watch_explainer.mp4)**

> [!IMPORTANT]
> **🚀 Deploy the Production Enterprise Engine (`AgentControl`)**  
> Looking for the complete production concurrency suite with Redis clustering, FastMCP drop-in decorators, and Prometheus telemetry?  
> 📦 **Get [AgentControl Starter Kit ($149)](https://balsarohith.gumroad.com/l/vnztsy)** (Commercial License) or get the **[LeadHunter Pro 5,000 AI Founder Engine ($97)](https://balsarohith.gumroad.com/l/LeadHunter)**.

---

## The Problem: The Multi-Agent Lost Update Crisis

In distributed multi-agent workflows (e.g. LangGraph subagent swarms, FastMCP tool servers, AutoGen pipelines), multiple agents execute in parallel. Because LLM generation and tool execution take between **500ms and 5,000ms**, standard state management patterns fail:

```
Agent A (reads v1) ──[ LLM Reasoning 1.2s ]───────► Commits delta A (Advances state to v2)
Agent B (reads v1) ──────────[ LLM Reasoning 2.1s ]──────────────► Overwrites state! (Delta A LOST)
```

1. **Naive In-Place Overwrites**: The slowest agent stomps on all previous mutations. In a 25-agent concurrent swarm, **96% of intermediate agent updates are silently dropped**.
2. **Pessimistic Mutexes**: Holding a distributed lock across the entire LLM roundtrip forces serial execution, ballooning p95 latency by 10x–25x.
3. **Distributed Deadlocks**: If an agent times out, crashes, or gets rate-limited by an API while holding a pessimistic lock, downstream workers starve.

## The Solution: `agent-occ-lock`

`agent-occ-lock` implements lightweight **Optimistic Concurrency Control (OCC)** paired with **3-Way Semantic Merging**:

1. **Compare-And-Swap (CAS) Tokens**: Every state read yields an immutable revision tag (`version`, `etag`). Writes enforce precondition validation.
2. **3-Way Delta Auto-Merge**: When a conflict occurs (`CASMismatch`), `AgentOCCStore` computes field-level deltas against the base state. If the concurrent agents touched orthogonal keys (e.g., Agent 1 wrote `sentiment` and Agent 2 wrote `summary`), the delta is **merged automatically without re-invoking the LLM**.
3. **Jittered Backoff Retries**: For genuine field collisions, an asynchronous retry loop re-evaluates the worker function with freshly injected state.
4. **Zero External Dependencies**: Pure Python standard library (`asyncio`, `hashlib`, `json`).

---

## Concurrency Benchmark Results

Simulated 25 concurrent agent workers executing parallel reads, 25ms inference latency, and shared state mutations:

| Metric | Naive In-Place Overwrite | `agent-occ-lock` (CAS + 3-Way Merge) |
| :--- | :--- | :--- |
| **Concurrent Workers** | 25 | 25 |
| **Retained Mutations** | 1 / 25 | **25 / 25** |
| **Lost Updates (Silent Loss)** | **24** | **0** |
| **Data Loss Rate** | **96.0%** | **0.0%** |
| **Wall Clock Execution** | 0.0350s | **0.0323s** |

Run the benchmark yourself:
```bash
python benchmark.py
```

---

## Architecture

```
                    ┌──────────────────────────────────┐
                    │       Shared Agent State         │
                    │   (Version: 4, ETag: 'a3f9e2')   │
                    └─────────────────┬────────────────┘
                                      │
               ┌──────────────────────┴──────────────────────┐
               ▼                                             ▼
        Agent 1 (Reads v4)                            Agent 2 (Reads v4)
     [ LLM Call: extracts tags ]                   [ LLM Call: writes summary ]
               │                                             │
               ▼                                             ▼
       CAS Commit: v4 -> v5                          CAS Commit: v4 (STALE!)
          (State becomes v5)                                 │
                                                             ▼
                                                    3-Way Semantic Merge
                                              ┌──────────────────────────────┐
                                              │ Keys orthogonal?             │
                                              │ - summary != tags -> MERGE!  │
                                              │ State becomes v6 (0 reruns)  │
                                              └──────────────────────────────┘
```

---

## Quickstart

### Installation

```bash
pip install agent-occ-lock
```
*(Or clone locally for zero-dependency drop-in)*

### Basic Optimistic Commit

```python
from agent_occ import AgentOCCStore

store = AgentOCCStore()

# 1. Initialize shared state
snapshot = store.get_or_create("doc_eval_42", {"status": "pending", "score": 0})
print(f"Read version: {snapshot.version}")  # 1

# 2. Worker reads and prepares mutation
worker_expected_version = snapshot.version
delta = {"status": "analyzed", "score": 88}

# 3. Atomic CAS commit
result = store.commit(
    key="doc_eval_42",
    expected_version=worker_expected_version,
    delta=delta,
    agent_id="evaluator_agent",
)

if result.success:
    print(f"Committed! New version: {result.record.version}")
```

### High-Level Async Swarm Worker (Auto-Retry + 3-Way Merge)

```python
import asyncio
from agent_occ import AgentOCCStore

store = AgentOCCStore()

async def run_analysis_agent(agent_name: str, metric_name: str, metric_value: int):
    async def mutate_fn(current_state: dict):
        # Simulate LLM tool execution / inference
        await asyncio.sleep(0.05)
        return {metric_name: metric_value}

    result = await store.mutate_async(
        key="pipeline_run_01",
        mutate_fn=mutate_fn,
        agent_id=agent_name,
        max_retries=5,
    )
    return result

async def main():
    # Run 5 concurrent subagents mutating shared pipeline state
    agents = [
        run_analysis_agent(f"agent_{i}", f"feature_{i}", i * 10)
        for i in range(5)
    ]
    await asyncio.gather(*agents)

    # 100% of subagent writes are preserved without race conditions
    final = store.read("pipeline_run_01")
    print(final.data)

asyncio.run(main())
```

### Cryptographic Audit Ledger

Every atomic transition is logged in the append-only ledger:

```python
trail = store.get_audit_trail("pipeline_run_01")
for event in trail:
    print(f"v{event.from_version} -> v{event.to_version} by {event.agent_id} | diff: {event.diff}")
```

---

## Running Tests

```bash
pytest -v
```

Output:
```text
tests/test_concurrency.py::test_basic_lifecycle PASSED
tests/test_concurrency.py::test_stale_commit_rejection_without_merge PASSED
tests/test_concurrency.py::test_three_way_auto_merge_disjoint_fields PASSED
tests/test_concurrency.py::test_three_way_collision_detection PASSED
tests/test_concurrency.py::test_concurrent_agent_swarm_zero_lost_updates PASSED
tests/test_mcp_server.py::test_mcp_read_and_commit PASSED
```

---

## Model Context Protocol (MCP) Server

`agent-occ-lock` ships with native MCP 2.x support over `stdio`, enabling Claude Desktop, Cursor, and any MCP client to enforce optimistic concurrency over shared agent memory:

```bash
python -m agent_occ.mcp_server
```

### Exposed MCP Tools
- `read_agent_state(key)`: Returns state snapshot, revision version, and SHA-256 ETag.
- `commit_agent_state_occ(key, expected_version, delta, agent_id, auto_merge=True)`: Atomically commits state updates or reconciles disjoint deltas with 0% data loss.

---

## License

MIT © [Rohith G](https://github.com/rohith0707)
