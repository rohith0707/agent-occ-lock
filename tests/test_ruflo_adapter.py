"""Tests for Ruflo (Claude Flow) Swarm Coordinator integration."""

import asyncio
import unittest
from agent_occ.ruflo_adapter import RufloSwarmCoordinator


class TestRufloAdapter(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.coordinator = RufloSwarmCoordinator()

    def test_init_and_read_swarm_session(self):
        session = self.coordinator.init_swarm_session(
            session_id="ruflo_swarm_001",
            initial_state={"status": "initialized", "agents": []},
            coordinator_id="ruflo_lead"
        )
        self.assertEqual(session.version, 1)
        self.assertEqual(session.data["status"], "initialized")

        state = self.coordinator.read_swarm_state("ruflo_swarm_001")
        self.assertIsNotNone(state)
        self.assertEqual(state["revision"], 1)
        self.assertEqual(state["state"]["status"], "initialized")

    def test_disjoint_agent_auto_merge(self):
        self.coordinator.init_swarm_session(
            session_id="ruflo_swarm_002",
            initial_state={"stage": "research"}
        )

        # Worker 1 reads revision 1, updates repo_analysis
        res1 = self.coordinator.commit_agent_step(
            session_id="ruflo_swarm_002",
            agent_id="agent_repo_scanner",
            expected_revision=1,
            delta={"repo_analysis": {"stars": 81420, "language": "TypeScript"}}
        )
        self.assertTrue(res1.success)
        self.assertEqual(res1.record.version, 2)

        # Worker 2 started from revision 1 (stale!), updates tech_stack (disjoint key)
        res2 = self.coordinator.commit_agent_step(
            session_id="ruflo_swarm_002",
            agent_id="agent_stack_miner",
            expected_revision=1,
            delta={"tech_stack": ["FastMCP", "Redis", "LangGraph"]},
            auto_merge=True
        )
        self.assertTrue(res2.success)
        self.assertTrue(res2.auto_merged)
        self.assertEqual(res2.record.version, 3)

        # Both keys must be present in the state
        final_state = self.coordinator.read_swarm_state("ruflo_swarm_002")
        self.assertIn("repo_analysis", final_state["state"])
        self.assertIn("tech_stack", final_state["state"])

    async def test_reconcile_parallel_swarm_fanout(self):
        self.coordinator.init_swarm_session(
            session_id="ruflo_swarm_fanout",
            initial_state={"status": "processing"}
        )

        # 5 parallel agents reporting findings simultaneously
        mutations = [
            {"agent_id": f"worker_{i}", "delta": {f"task_output_{i}": f"completed_by_{i}"}}
            for i in range(5)
        ]

        result = await self.coordinator.reconcile_parallel_swarm(
            session_id="ruflo_swarm_fanout",
            agent_mutations=mutations
        )

        self.assertEqual(result["committed_agents"], 5)
        self.assertEqual(len(result["conflicts"]), 0)
        self.assertEqual(result["final_revision"], 6)

        # Check export manifest
        manifest = self.coordinator.export_ruflo_manifest("ruflo_swarm_fanout")
        self.assertEqual(manifest["schema_version"], "ruflo-occ-v1")
        self.assertEqual(manifest["transaction_count"], 5)
        self.assertEqual(len(manifest["active_agents"]), 5)


if __name__ == "__main__":
    unittest.main()
