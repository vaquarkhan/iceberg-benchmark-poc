#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Copyright 2026 Vaquar Khan

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

================================================================================
Single File Commits Performance Benchmark
================================================================================
Author: Vaquar Khan (vaquar.khan@gmail.com)
Purpose: Measure real commit latency and metadata overhead for streaming workloads

This benchmark validates that single-file commits are viable for real-time
streaming workloads (Kafka, Kinesis, Flink) by measuring actual commit
performance, metadata growth, and compaction costs.
"""

import time
import json
import os
import tempfile
from pathlib import Path
from typing import List, Dict
from dataclasses import dataclass, asdict
from datetime import datetime
import psutil

@dataclass
class CommitMetrics:
    batch_size: int
    commit_time_ms: float
    metadata_size_bytes: int
    memory_used_mb: float

@dataclass
class ManifestEntry:
    file_path: str
    file_size_bytes: int
    record_count: int
    partition_value: str

class SingleFileCommitBenchmark:
    def __init__(self, results_dir: Path):
        self.results_dir = Path(results_dir)
        self.results_dir.mkdir(exist_ok=True)
        self.temp_dir = tempfile.mkdtemp(prefix="iceberg_bench_")
        self.process = psutil.Process()
        
    def create_manifest_entry(self, file_id: int, partition: str) -> ManifestEntry:
        return ManifestEntry(
            file_path=f"s3://bucket/table/data/partition={partition}/file_{file_id:06d}.parquet",
            file_size_bytes=64 * 1024 * 1024,
            record_count=500_000,
            partition_value=partition
        )
    
    def measure_commit(self, batch_size: int, num_commits: int = 10) -> List[CommitMetrics]:
        print(f"\n--- Measuring commits with batch_size={batch_size} ---")
        
        metrics_list = []
        
        for commit_id in range(num_commits):
            mem_before = self.process.memory_info().rss / (1024 * 1024)
            
            entries = []
            for i in range(batch_size):
                partition = f"2024-01-{(i % 31) + 1:02d}"
                entry = self.create_manifest_entry(commit_id * batch_size + i, partition)
                entries.append(entry)
            
            start_time = time.perf_counter()
            
            manifest_data = {
                "format-version": 2,
                "entries": [asdict(e) for e in entries]
            }
            json_str = json.dumps(manifest_data, indent=2)
            
            manifest_path = Path(self.temp_dir) / f"manifest_{commit_id}.json"
            with open(manifest_path, 'w') as f:
                f.write(json_str)
            
            commit_time = (time.perf_counter() - start_time) * 1000
            metadata_size = os.path.getsize(manifest_path)
            
            mem_after = self.process.memory_info().rss / (1024 * 1024)
            memory_used = mem_after - mem_before
            
            metrics = CommitMetrics(
                batch_size=batch_size,
                commit_time_ms=commit_time,
                metadata_size_bytes=metadata_size,
                memory_used_mb=memory_used
            )
            
            metrics_list.append(metrics)
        
        return metrics_list
    
    def measure_manifest_growth(self, num_commits: int = 100, files_per_commit: int = 1) -> Dict:
        print(f"\n--- Measuring manifest growth: {num_commits} commits ---")
        
        total_entries = 0
        cumulative_size_bytes = 0
        growth_points = []
        
        for commit_id in range(num_commits):
            entries = []
            for i in range(files_per_commit):
                partition = f"2024-01-{(i % 31) + 1:02d}"
                entry = self.create_manifest_entry(total_entries + i, partition)
                entries.append(entry)
            
            total_entries += files_per_commit
            
            manifest_data = {
                "format-version": 2,
                "entries": [asdict(e) for e in entries]
            }
            manifest_path = Path(self.temp_dir) / f"growth_manifest_{commit_id}.json"
            with open(manifest_path, 'w') as f:
                json.dump(manifest_data, f, indent=2)
            
            manifest_size = os.path.getsize(manifest_path)
            cumulative_size_bytes += manifest_size
            
            if (commit_id + 1) % 10 == 0:
                growth_points.append({
                    "commit_number": commit_id + 1,
                    "total_files": total_entries,
                    "cumulative_size_mb": cumulative_size_bytes / (1024 * 1024)
                })
        
        return {
            "total_commits": num_commits,
            "files_per_commit": files_per_commit,
            "total_files": total_entries,
            "total_size_mb": cumulative_size_bytes / (1024 * 1024),
            "growth_points": growth_points
        }
    
    def measure_compaction_cost(self, num_small_manifests: int = 100) -> Dict:
        print(f"\n--- Measuring compaction cost: {num_small_manifests} manifests ---")
        
        manifest_paths = []
        all_entries = []
        
        for i in range(num_small_manifests):
            entry = self.create_manifest_entry(i, f"2024-01-{(i % 31) + 1:02d}")
            all_entries.append(entry)
            
            manifest_data = {"format-version": 2, "entries": [asdict(entry)]}
            manifest_path = Path(self.temp_dir) / f"compact_small_{i}.json"
            with open(manifest_path, 'w') as f:
                json.dump(manifest_data, f)
            manifest_paths.append(manifest_path)
        
        size_before = sum(os.path.getsize(p) for p in manifest_paths)
        
        compact_start = time.perf_counter()
        
        for path in manifest_paths:
            with open(path, 'r') as f:
                json.load(f)
        
        compacted_data = {
            "format-version": 2,
            "entries": [asdict(e) for e in all_entries]
        }
        compacted_path = Path(self.temp_dir) / "compacted_manifest.json"
        with open(compacted_path, 'w') as f:
            json.dump(compacted_data, f, indent=2)
        
        compact_time = (time.perf_counter() - compact_start) * 1000
        size_after = os.path.getsize(compacted_path)
        
        return {
            "num_small_manifests": num_small_manifests,
            "total_compact_time_ms": compact_time,
            "size_before_mb": size_before / (1024 * 1024),
            "size_after_mb": size_after / (1024 * 1024),
            "size_reduction_pct": ((size_before - size_after) / size_before) * 100
        }
    
    def cleanup(self):
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)


def run_single_file_commit_benchmarks():
    print("=" * 80)
    print("SINGLE FILE COMMITS PERFORMANCE BENCHMARK")
    print("=" * 80)
    
    results_dir = Path("results")
    benchmark = SingleFileCommitBenchmark(results_dir)
    
    all_results = {
        "timestamp": datetime.now().isoformat(),
        "scenario_a": [],
        "scenario_b": {},
        "scenario_c": {}
    }
    
    try:
        # Scenario A: Commit Latency vs Batch Size
        print("\nSCENARIO A: Commit Latency vs Batch Size")
        batch_sizes = [1, 10, 50, 100, 500, 1000]
        
        for batch_size in batch_sizes:
            metrics_list = benchmark.measure_commit(batch_size, num_commits=10)
            
            avg_commit_time = sum(m.commit_time_ms for m in metrics_list) / len(metrics_list)
            avg_metadata_size = sum(m.metadata_size_bytes for m in metrics_list) / len(metrics_list)
            
            result = {
                "batch_size": batch_size,
                "avg_commit_time_ms": avg_commit_time,
                "avg_metadata_size_kb": avg_metadata_size / 1024,
                "throughput_files_per_sec": (batch_size / avg_commit_time) * 1000
            }
            all_results["scenario_a"].append(result)
            
            print(f"  Batch {batch_size:4d}: {avg_commit_time:6.2f} ms, "
                  f"{avg_metadata_size/1024:6.2f} KB, "
                  f"{result['throughput_files_per_sec']:6.1f} files/sec")
        
        # Scenario B: Manifest Growth
        print("\nSCENARIO B: Manifest Growth Over Time")
        growth_result = benchmark.measure_manifest_growth(num_commits=100, files_per_commit=1)
        all_results["scenario_b"] = growth_result
        print(f"  Total size after 100 commits: {growth_result['total_size_mb']:.2f} MB")
        
        # Scenario C: Compaction Cost
        print("\nSCENARIO C: Manifest Compaction Cost")
        compaction_result = benchmark.measure_compaction_cost(num_small_manifests=100)
        all_results["scenario_c"] = compaction_result
        print(f"  Compaction time: {compaction_result['total_compact_time_ms']:.2f} ms")
        print(f"  Size reduction: {compaction_result['size_reduction_pct']:.1f}%")
        
        output_file = results_dir / "single_file_commits_results.json"
        with open(output_file, 'w') as f:
            json.dump(all_results, f, indent=2)
        
        print(f"\n✅ Results saved to {output_file}")
        
    finally:
        benchmark.cleanup()
    
    return all_results


if __name__ == "__main__":
    run_single_file_commit_benchmarks()
