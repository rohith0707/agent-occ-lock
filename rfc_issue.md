## Background & Problem Statement

In distributed multi-agent architectures (e.g. parallel LangGraph subagent swarms, FastMCP tool servers, CrewAI pipelines), multiple worker agents execute concurrently. Because LLM tool execution and reasoning typically take between 500ms and 5,000ms, standard shared-state implementations suffer from the classic **Lost Update Anomaly**:

```
Agent 1 (reads v1) ──[ Model Reasoning 1.2s ]───────► Commits delta 1 (Advances state to v2)
Agent 2 (reads v1) ──────────[ Model Reasoning 2.1s ]──────────────► Overwrites state! (Delta 1 LOST)
```

1. **Naive In-Place Overwrites**: In our 25-worker test, 24 out of 25 writes were silently wiped out (**96.0% data loss**).
2. **Pessimistic Distributed Locks (Redis Redlock)**: Holding a mutex across the entire model inference round-trip creates an unacceptable latency bottleneck (10x-25x p95 explosion) and introduces deadlock risks if a worker times out or crashes.

---

## Benchmark Results

We simulated 25 concurrent agent workers executing parallel reads, 25ms inference latency, and shared state mutations:

| Metric | Naive In-Place Overwrite | `agent-occ-lock` (CAS + 3-Way Merge) |
| :--- | :--- | :--- |
| **Concurrent Workers** | 25 | 25 |
| **Retained Mutations** | 1 / 25 | **25 / 25** |
| **Lost Updates (Silent Loss)** | **24** | **0** |
| **Data Loss Rate** | **96.0%** | **0.0%** |
| **Wall Clock Execution** | 0.0350s | **0.0323s** |

---

## Proposed RFC Architecture
This repository provides a zero-dependency reference implementation:
1. **Compare-And-Swap (CAS) Revision Tokens**: Every state read yields an immutable version tag (`version`, `etag`). Writes enforce precondition validation.
2. **3-Way Semantic Auto-Merge**: When a conflict occurs (`CASMismatch`), `AgentOCCStore` computes field-level deltas against the base snapshot. If the concurrent agents touched orthogonal keys, the delta is merged automatically without re-invoking the LLM.
3. **Jittered Backoff Retries**: For genuine field collisions, an asynchronous retry loop re-evaluates the worker function with freshly injected state.

---

## Discussion Questions for Distributed Systems & AI Engineers
1. How does your production agent runtime isolate shared memory across parallel subagents?
2. Do you rely on checkpoint replay, CRDTs, or optimistic version checks?
3. What edge cases have you encountered with 3-way delta reconciliation when subagents modify nested list indices?

Looking forward to hearing from practitioners and maintainers!
