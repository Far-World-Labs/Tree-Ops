"""Simple performance test runner using httpx directly."""

import asyncio
import time
from dataclasses import dataclass
from typing import Any

import httpx
import numpy as np
import psutil
from rich.console import Console
from rich.table import Table

from app.ops.perf.db_utils import DatabaseManager
from app.ops.perf.generator import balanced_tree, linear_chain, star_tree


@dataclass
class TestScenario:
    """Test scenario definition."""

    name: str
    org_id: str
    node_count: int
    tree_count: int
    tree_shape: str
    concurrent_users: int
    duration_seconds: int
    read_ratio: float
    write_pattern: str = "simple"  # simple, deep, mixed, delete, move


@dataclass
class TestResult:
    """Test result metrics."""

    scenario_name: str
    node_count: int
    tree_count: int
    tree_depth: int
    tree_shape: str
    read_write_ratio: str
    requests: int
    rps: float
    success_rate: float
    p50: float
    p95: float
    p99: float
    ms_per_node: float
    response_size_kb: float
    cpu_percent: float
    index_scans: int
    seq_scans: int


class PerformanceRunner:
    """Runs performance tests."""

    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.console = Console()
        self.db_manager = DatabaseManager()

    async def run_suite(self, scenarios: list[TestScenario]) -> list[TestResult]:
        """Run a suite of test scenarios."""
        results = []

        self.console.print("[bold blue]Performance Test Suite[/bold blue]\n")

        # Prepare database before running tests
        self.db_manager.prepare_for_test()

        for scenario in scenarios:
            self.console.print(f"Running {scenario.name} ({scenario.node_count} nodes)...")

            try:
                result = await self._run_scenario(scenario)
                results.append(result)
                self.console.print(f"  ✓ {scenario.name}: {result.rps:.1f} RPS, P95={result.p95:.0f}ms")
            except Exception as e:
                self.console.print(f"  ✗ {scenario.name}: [red]{str(e)}[/red]")

        return results

    async def _run_scenario(self, scenario: TestScenario) -> TestResult:
        """Run a single scenario."""

        # Kill any runaway queries before starting
        self.db_manager.kill_queries(max_age_seconds=3)

        # Reset stats before scenario
        self.db_manager.reset_stats()

        # Clean org data
        async with httpx.AsyncClient(base_url=self.base_url, timeout=30.0) as client:
            await client.delete("/api/tree", headers={"org-id": scenario.org_id})

        # Setup test data
        test_data = self._generate_data(scenario)
        async with httpx.AsyncClient(base_url=self.base_url, timeout=60.0) as client:
            for i in range(0, len(test_data), 1000):
                chunk = test_data[i : i + 1000]
                await client.post("/api/tree/bulk", json=chunk, headers={"org-id": scenario.org_id})

        # Measure baseline response
        async with httpx.AsyncClient(base_url=self.base_url, timeout=60.0) as client:
            response = await client.get("/api/tree", headers={"org-id": scenario.org_id})
            response_size = len(response.content)

        # Run load test
        response_times = []
        successful = 0
        failed = 0

        start_time = time.time()
        end_time = start_time + scenario.duration_seconds

        # Create concurrent tasks
        tasks = []
        for _ in range(scenario.concurrent_users):
            task = asyncio.create_task(
                self._user_session(
                    scenario.org_id, end_time, scenario.read_ratio, response_times, scenario.write_pattern
                )
            )
            tasks.append(task)

        # Wait for completion
        results = await asyncio.gather(*tasks)

        for success, fail in results:
            successful += success
            failed += fail

        # Calculate metrics
        duration = time.time() - start_time
        rps = (successful + failed) / duration if duration > 0 else 0
        success_rate = successful / (successful + failed) if (successful + failed) > 0 else 0

        # Calculate percentiles
        if response_times:
            p50 = np.percentile(response_times, 50)
            p95 = np.percentile(response_times, 95)
            p99 = np.percentile(response_times, 99)
        else:
            p50 = p95 = p99 = 0

        # Get CPU usage
        cpu_percent = psutil.cpu_percent(interval=0.1)

        # Get database stats
        db_stats = self.db_manager.get_table_stats()

        # Calculate ms per node
        ms_per_node = p95 / scenario.node_count if scenario.node_count > 0 and p95 > 0 else 0

        # Determine tree depth based on shape
        if scenario.tree_shape == "deep":
            tree_depth = scenario.node_count
        elif scenario.tree_shape == "wide":
            tree_depth = 2
        else:  # balanced
            import math

            tree_depth = int(math.log(scenario.node_count, 3)) + 1 if scenario.node_count > 0 else 0

        # Format read/write ratio
        read_pct = int(scenario.read_ratio * 100)
        write_pct = 100 - read_pct
        rw_ratio = f"{read_pct}:{write_pct}"

        # Clean up
        async with httpx.AsyncClient(base_url=self.base_url, timeout=30.0) as client:
            await client.delete("/api/tree", headers={"org-id": scenario.org_id})

        return TestResult(
            scenario_name=scenario.name,
            node_count=scenario.node_count,
            tree_count=scenario.tree_count,
            tree_depth=tree_depth,
            tree_shape=scenario.tree_shape,
            read_write_ratio=rw_ratio,
            requests=successful + failed,
            rps=rps,
            success_rate=success_rate,
            p50=p50,
            p95=p95,
            p99=p99,
            ms_per_node=ms_per_node,
            response_size_kb=response_size / 1024,
            cpu_percent=cpu_percent,
            index_scans=db_stats["idx_scans"],
            seq_scans=db_stats["seq_scans"],
        )

    async def _user_session(
        self,
        org_id: str,
        end_time: float,
        read_ratio: float,
        response_times: list[float],
        write_pattern: str = "simple",
    ) -> tuple[int, int]:
        """Simulate a user session."""
        successful = 0
        failed = 0

        # For deep writes, get some existing nodes
        existing_nodes = []
        if write_pattern in ["deep", "mixed"]:
            async with httpx.AsyncClient(base_url=self.base_url, timeout=30.0) as client:
                try:
                    resp = await client.get("/api/tree", headers={"org-id": org_id})
                    if resp.status_code == 200:
                        existing_nodes = [n["id"] for n in resp.json()[:20]]
                except Exception:
                    pass

        async with httpx.AsyncClient(base_url=self.base_url, timeout=30.0) as client:
            while time.time() < end_time:
                is_read = np.random.random() < read_ratio

                try:
                    start = time.time()

                    if is_read:
                        response = await client.get("/api/tree", headers={"org-id": org_id})
                    else:
                        # Handle move pattern (30% moves, 70% inserts)
                        if write_pattern == "move" and len(existing_nodes) > 1 and np.random.random() < 0.3:
                            # Move a subtree to a different parent
                            node_id = np.random.choice(existing_nodes)
                            new_parent = np.random.choice([n for n in existing_nodes if n != node_id])
                            response = await client.patch(
                                f"/api/tree/{node_id}", json={"parentId": new_parent}, headers={"org-id": org_id}
                            )
                        else:
                            # Regular insert
                            parent_id = None
                            if write_pattern == "deep" and existing_nodes:
                                parent_id = np.random.choice(existing_nodes)
                            elif write_pattern == "mixed" and existing_nodes and np.random.random() < 0.5:
                                parent_id = np.random.choice(existing_nodes)

                            response = await client.post(
                                "/api/tree",
                                json={"label": f"Node_{int(time.time() * 1000000)}", "parentId": parent_id},
                                headers={"org-id": org_id},
                            )

                    elapsed_ms = (time.time() - start) * 1000

                    if response.status_code in [200, 201]:
                        successful += 1
                        response_times.append(elapsed_ms)
                    else:
                        failed += 1

                except Exception:
                    failed += 1

        return successful, failed

    def _generate_data(self, scenario: TestScenario) -> list[dict[str, Any]]:
        """Generate test data."""
        import random

        base_id = int(time.time() * 1000000) + random.randint(0, 99999)

        if scenario.tree_shape == "deep":
            nodes = linear_chain(base_id, scenario.node_count)
        elif scenario.tree_shape == "wide":
            nodes = star_tree(base_id, scenario.node_count - 1)
        else:
            nodes = balanced_tree(base_id, scenario.node_count, branching=3)

        # Handle multiple trees
        if scenario.tree_count > 1:
            nodes_per_tree = max(1, scenario.node_count // scenario.tree_count)
            all_nodes = []
            for i in range(scenario.tree_count):
                tree_start = base_id + (i * 1000000)
                tree_nodes = balanced_tree(tree_start, nodes_per_tree, branching=3)
                all_nodes.extend(tree_nodes)
            nodes = all_nodes[: scenario.node_count]

        return nodes

    def display_results(self, results: list[TestResult]):
        """Display results table."""
        table = Table(title="Performance Test Results")

        table.add_column("Scenario", style="cyan")
        table.add_column("Nodes", justify="right")
        table.add_column("Depth", justify="right")
        table.add_column("Trees", justify="right")
        table.add_column("R:W", justify="center")
        table.add_column("RPS", justify="right")
        table.add_column("P50", justify="right")
        table.add_column("P95", justify="right")
        table.add_column("P99", justify="right")
        table.add_column("ms/node", justify="right")
        table.add_column("IDX", justify="right")
        table.add_column("SEQ", justify="right")
        table.add_column("CPU%", justify="right")

        for r in results:
            # Color coding
            rps_style = "green" if r.rps > 100 else "yellow" if r.rps > 50 else "red"
            p95_style = "green" if r.p95 < 200 else "yellow" if r.p95 < 1000 else "red"
            ms_style = "green" if r.ms_per_node < 0.5 else "yellow" if r.ms_per_node < 2 else "red"
            idx_style = "green" if r.index_scans > r.seq_scans else "red"

            table.add_row(
                r.scenario_name,
                str(r.node_count),
                str(r.tree_depth),
                str(r.tree_count),
                r.read_write_ratio,
                f"[{rps_style}]{r.rps:.1f}[/{rps_style}]",
                f"{r.p50:.0f}",
                f"[{p95_style}]{r.p95:.0f}[/{p95_style}]",
                f"{r.p99:.0f}",
                f"[{ms_style}]{r.ms_per_node:.2f}[/{ms_style}]",
                f"[{idx_style}]{r.index_scans}[/{idx_style}]",
                str(r.seq_scans),
                f"{r.cpu_percent:.0f}",
            )

        self.console.print("\n")
        self.console.print(table)

        # Analysis
        if len(results) >= 2:
            self.console.print("\n[bold]Analysis:[/bold]")

            # Find scale degradation
            small = next((r for r in results if r.node_count <= 1000), None)
            large = next((r for r in results if r.node_count >= 10000), None)

            if small and large:
                degradation = (1 - large.rps / small.rps) * 100 if small.rps > 0 else 0
                self.console.print(f"  • Scale degradation: {degradation:.1f}%")

            # Forest optimization
            forest = [r for r in results if r.tree_count > 1]
            if forest:
                best = max(forest, key=lambda x: x.rps)
                single = next((r for r in results if r.node_count == best.node_count and r.tree_count == 1), None)
                if single:
                    improvement = ((best.rps - single.rps) / single.rps * 100) if single.rps > 0 else 0
                    self.console.print(f"  • Forest optimization: {improvement:.1f}% with {best.tree_count} trees")
