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
DV Resolution Strategies Benchmark
================================================================================
Author: Vaquar Khan (vaquar.khan@gmail.com)
Purpose: Validate architectural choices for V4 DV-to-data-file resolution

Addresses Apache Iceberg community discussion:
- Anton Okolnychyi: Path-based hash join overhead concern
- Anoop Johnson: Positional join alternative proposal
- Steven Wu: Column-split Parquet / coalesced join proposal

Test Scenarios:
A. Hash Join vs Positional Join Performance
B. I/O Reduction with Folded DVs
C. Coalesced Join with Multiple DV Manifests
D. Write Overhead for Order-Preserving Manifests
"""

import time
import json
from typing import List, Dict, Tuple
from dataclasses import dataclass
from utils.s3_simulator import S3LatencySimulator
from utils.metrics_collector import MetricsCollector

@dataclass
class DataFileEntry:
    """Represents a data file in a manifest"""
    file_path: str
    file_size_bytes: int
    record_count: int
    has_dv: bool

@dataclass
class DVEntry:
    """Represents a DV entry in a delete manifest"""
    file_path: str
    dv_size_bytes: int
    deleted_row_count: int

class DVResolutionBenchmark:
    """Benchmark suite for DV resolution strategies"""
    
    def __init__(self):
        self.s3 = S3LatencySimulator()
        self.metrics = MetricsCollector()
        
    def generate_manifests(self, num_files: int, dv_ratio: float = 0.3) -> Tuple[List[DataFileEntry], List[DVEntry]]:
        """
        Generate data and delete manifests
        
        Args:
            num_files: Number of data files
            dv_ratio: Ratio of files with DVs (0.0 to 1.0)
        """
        import random
        
        data_files = []
        dv_entries = []
        
        for i in range(num_files):
            file_path = f"s3://bucket/table/data/file_{i:06d}.parquet"
            has_dv = random.random() < dv_ratio
            
            data_file = DataFileEntry(
                file_path=file_path,
                file_size_bytes=random.randint(50_000_000, 200_000_000),  # 50-200MB
                record_count=random.randint(100_000, 500_000),
                has_dv=has_dv
            )
            data_files.append(data_file)
            
            if has_dv:
                dv_entry = DVEntry(
                    file_path=file_path,
                    dv_size_bytes=random.randint(100, 10_000),  # 100B-10KB
                    deleted_row_count=random.randint(1, 1000)
                )
                dv_entries.append(dv_entry)
        
        return data_files, dv_entries
    
    def benchmark_hash_join(self, data_files: List[DataFileEntry], dv_entries: List[DVEntry]) -> Dict:
        """
        Benchmark path-based hash join (current V4 proposal)
        
        Simulates: Building hash table on file paths and probing
        """
        start_time = time.perf_counter()
        
        # Build hash table (delete manifest)
        dv_map = {}
        for dv in dv_entries:
            dv_map[dv.file_path] = dv
        
        build_time = time.perf_counter() - start_time
        
        # Probe hash table (data manifest)
        probe_start = time.perf_counter()
        matched_files = []
        for data_file in data_files:
            if data_file.file_path in dv_map:
                matched_files.append((data_file, dv_map[data_file.file_path]))
        
        probe_time = time.perf_counter() - probe_start
        total_time = time.perf_counter() - start_time
        
        # Memory estimation (hash table overhead)
        # Each entry: ~200 bytes (path string + pointer + hash)
        memory_bytes = len(dv_map) * 200
        
        return {
            "strategy": "hash_join",
            "total_time_ms": total_time * 1000,
            "build_time_ms": build_time * 1000,
            "probe_time_ms": probe_time * 1000,
            "memory_bytes": memory_bytes,
            "matched_files": len(matched_files),
            "hash_table_size": len(dv_map)
        }
    
    def benchmark_positional_join(self, data_files: List[DataFileEntry], dv_entries: List[DVEntry]) -> Dict:
        """
        Benchmark positional join (Anoop's proposal)
        
        Simulates: Order-preserving manifests with NULL for missing DVs
        """
        start_time = time.perf_counter()
        
        # Create order-preserving DV list (with NULLs)
        dv_map = {dv.file_path: dv for dv in dv_entries}
        ordered_dvs = []
        for data_file in data_files:
            if data_file.file_path in dv_map:
                ordered_dvs.append(dv_map[data_file.file_path])
            else:
                ordered_dvs.append(None)  # NULL for files without DVs
        
        # Positional join (simple zip)
        matched_files = []
        for i, data_file in enumerate(data_files):
            if ordered_dvs[i] is not None:
                matched_files.append((data_file, ordered_dvs[i]))
        
        total_time = time.perf_counter() - start_time
        
        # Memory estimation (array-based, no hash overhead)
        # Each entry: ~8 bytes (pointer only)
        memory_bytes = len(data_files) * 8
        
        return {
            "strategy": "positional_join",
            "total_time_ms": total_time * 1000,
            "build_time_ms": 0,  # No hash table build
            "probe_time_ms": total_time * 1000,  # All time is "probe"
            "memory_bytes": memory_bytes,
            "matched_files": len(matched_files),
            "null_entries": len(data_files) - len(dv_entries)
        }
    
    def benchmark_io_reduction(self, num_files: int, dv_ratio: float) -> Dict:
        """
        Benchmark I/O reduction with folded DVs
        
        Compares:
        - Separate manifests: Read data manifest + delete manifest
        - Folded DVs: Read single manifest with DVs as column
        """
        data_files, dv_entries = self.generate_manifests(num_files, dv_ratio)
        
        # Scenario A: Separate manifests (current V4)
        data_manifest_size = num_files * 200  # ~200 bytes per entry
        dv_manifest_size = len(dv_entries) * 150  # ~150 bytes per DV entry
        
        separate_start = time.perf_counter()
        # Simulate S3 reads
        self.s3.simulate_read(data_manifest_size)
        self.s3.simulate_read(dv_manifest_size)
        separate_time = time.perf_counter() - separate_start
        separate_requests = 2
        
        # Scenario B: Folded DVs (DVs as column in data manifest)
        folded_manifest_size = data_manifest_size + dv_manifest_size
        
        folded_start = time.perf_counter()
        # Simulate single S3 read
        self.s3.simulate_read(folded_manifest_size)
        folded_time = time.perf_counter() - folded_start
        folded_requests = 1
        
        # Calculate I/O reduction
        io_reduction_pct = ((separate_requests - folded_requests) / separate_requests) * 100
        time_reduction_pct = ((separate_time - folded_time) / separate_time) * 100
        
        return {
            "num_files": num_files,
            "dv_ratio": dv_ratio,
            "separate_manifests": {
                "requests": separate_requests,
                "time_ms": separate_time * 1000,
                "data_manifest_size_kb": data_manifest_size / 1024,
                "dv_manifest_size_kb": dv_manifest_size / 1024
            },
            "folded_dvs": {
                "requests": folded_requests,
                "time_ms": folded_time * 1000,
                "manifest_size_kb": folded_manifest_size / 1024
            },
            "io_reduction_pct": io_reduction_pct,
            "time_reduction_pct": time_reduction_pct
        }
    
    def benchmark_coalesced_join(self, num_files: int, num_dv_manifests: int) -> Dict:
        """
        Benchmark coalesced join with multiple DV manifests
        
        Simulates: Multiple affiliated DV manifests with COALESCED positional join
        (pick first non-null value as the DV)
        """
        import random
        
        # Generate data files
        data_files = [
            DataFileEntry(
                file_path=f"s3://bucket/table/data/file_{i:06d}.parquet",
                file_size_bytes=random.randint(50_000_000, 200_000_000),
                record_count=random.randint(100_000, 500_000),
                has_dv=True
            )
            for i in range(num_files)
        ]
        
        # Generate multiple DV manifests (simulating incremental updates)
        dv_manifests = []
        for manifest_idx in range(num_dv_manifests):
            # Each manifest covers different subset of files
            manifest_dvs = []
            for i, data_file in enumerate(data_files):
                # Randomly assign DVs to different manifests
                if random.random() < 0.3:  # 30% of files in this manifest
                    manifest_dvs.append(DVEntry(
                        file_path=data_file.file_path,
                        dv_size_bytes=random.randint(100, 10_000),
                        deleted_row_count=random.randint(1, 1000)
                    ))
                else:
                    manifest_dvs.append(None)
            dv_manifests.append(manifest_dvs)
        
        # Benchmark coalesced join
        start_time = time.perf_counter()
        
        coalesced_dvs = []
        for i in range(num_files):
            # Pick first non-null DV across all manifests
            dv = None
            for manifest in dv_manifests:
                if manifest[i] is not None:
                    dv = manifest[i]
                    break
            coalesced_dvs.append(dv)
        
        total_time = time.perf_counter() - start_time
        
        # Count non-null DVs
        resolved_dvs = sum(1 for dv in coalesced_dvs if dv is not None)
        
        return {
            "num_files": num_files,
            "num_dv_manifests": num_dv_manifests,
            "coalesce_time_ms": total_time * 1000,
            "resolved_dvs": resolved_dvs,
            "resolution_rate_pct": (resolved_dvs / num_files) * 100
        }
    
    def benchmark_write_overhead(self, num_files: int, dv_ratio: float) -> Dict:
        """
        Benchmark write overhead for order-preserving manifests
        
        Compares:
        - Unordered: Append DVs as they arrive
        - Order-preserving: Sort DVs to match data file order
        """
        import random
        
        data_files, dv_entries = self.generate_manifests(num_files, dv_ratio)
        
        # Scenario A: Unordered write (simple append)
        unordered_start = time.perf_counter()
        unordered_manifest = list(dv_entries)  # Just append
        unordered_time = time.perf_counter() - unordered_start
        
        # Scenario B: Order-preserving write (requires sorting)
        ordered_start = time.perf_counter()
        
        # Build position map from data files
        position_map = {df.file_path: i for i, df in enumerate(data_files)}
        
        # Sort DVs by position
        ordered_dvs = sorted(dv_entries, key=lambda dv: position_map.get(dv.file_path, float('inf')))
        
        # Insert NULLs for missing positions
        ordered_manifest = []
        dv_idx = 0
        for i, data_file in enumerate(data_files):
            if dv_idx < len(ordered_dvs) and ordered_dvs[dv_idx].file_path == data_file.file_path:
                ordered_manifest.append(ordered_dvs[dv_idx])
                dv_idx += 1
            else:
                ordered_manifest.append(None)  # NULL
        
        ordered_time = time.perf_counter() - ordered_start
        
        # Calculate overhead
        overhead_pct = ((ordered_time - unordered_time) / unordered_time) * 100 if unordered_time > 0 else 0
        
        return {
            "num_files": num_files,
            "dv_ratio": dv_ratio,
            "num_dvs": len(dv_entries),
            "unordered_write": {
                "time_ms": unordered_time * 1000,
                "manifest_entries": len(unordered_manifest)
            },
            "ordered_write": {
                "time_ms": ordered_time * 1000,
                "manifest_entries": len(ordered_manifest),
                "null_entries": len(ordered_manifest) - len(dv_entries)
            },
            "overhead_pct": overhead_pct
        }


def run_all_dv_resolution_benchmarks():
    """Run complete DV resolution strategy benchmark suite"""
    
    print("=" * 80)
    print("DV RESOLUTION STRATEGIES BENCHMARK SUITE")
    print("Validating V4 Architectural Choices")
    print("=" * 80)
    print()
    
    benchmark = DVResolutionBenchmark()
    results = {
        "scenario_a": [],
        "scenario_b": [],
        "scenario_c": [],
        "scenario_d": []
    }
    
    # ========================================================================
    # SCENARIO A: Hash Join vs Positional Join Performance
    # ========================================================================
    print("=" * 80)
    print("SCENARIO A: Hash Join vs Positional Join Performance")
    print("=" * 80)
    print()
    
    test_sizes = [1_000, 10_000, 25_000, 100_000]
    
    for num_files in test_sizes:
        print(f"--- Testing {num_files:,} manifest entries ---")
        
        data_files, dv_entries = benchmark.generate_manifests(num_files, dv_ratio=0.3)
        
        # Hash join
        hash_result = benchmark.benchmark_hash_join(data_files, dv_entries)
        print(f"  Hash Join: {hash_result['total_time_ms']:.2f} ms "
              f"(build: {hash_result['build_time_ms']:.2f} ms, "
              f"probe: {hash_result['probe_time_ms']:.2f} ms)")
        print(f"    Memory: {hash_result['memory_bytes'] / 1024:.2f} KB")
        
        # Positional join
        pos_result = benchmark.benchmark_positional_join(data_files, dv_entries)
        print(f"  Positional Join: {pos_result['total_time_ms']:.2f} ms")
        print(f"    Memory: {pos_result['memory_bytes'] / 1024:.2f} KB")
        
        # Calculate speedup
        speedup = hash_result['total_time_ms'] / pos_result['total_time_ms']
        memory_reduction = ((hash_result['memory_bytes'] - pos_result['memory_bytes']) / 
                           hash_result['memory_bytes']) * 100
        
        print(f"  🎯 Speedup: {speedup:.2f}x faster with positional join")
        print(f"  💾 Memory reduction: {memory_reduction:.1f}%")
        print()
        
        results["scenario_a"].append({
            "num_files": num_files,
            "hash_join": hash_result,
            "positional_join": pos_result,
            "speedup": speedup,
            "memory_reduction_pct": memory_reduction
        })
    
    # ========================================================================
    # SCENARIO B: I/O Reduction with Folded DVs
    # ========================================================================
    print("=" * 80)
    print("SCENARIO B: I/O Reduction with Folded DVs")
    print("=" * 80)
    print()
    
    test_configs = [
        (10_000, 0.1),   # 10% files with DVs
        (10_000, 0.3),   # 30% files with DVs
        (10_000, 0.5),   # 50% files with DVs
        (25_000, 0.3),   # Larger manifest
    ]
    
    for num_files, dv_ratio in test_configs:
        print(f"--- Testing {num_files:,} files, {dv_ratio*100:.0f}% with DVs ---")
        
        io_result = benchmark.benchmark_io_reduction(num_files, dv_ratio)
        
        print(f"  Separate Manifests:")
        print(f"    Requests: {io_result['separate_manifests']['requests']}")
        print(f"    Time: {io_result['separate_manifests']['time_ms']:.2f} ms")
        print(f"    Data manifest: {io_result['separate_manifests']['data_manifest_size_kb']:.2f} KB")
        print(f"    DV manifest: {io_result['separate_manifests']['dv_manifest_size_kb']:.2f} KB")
        
        print(f"  Folded DVs:")
        print(f"    Requests: {io_result['folded_dvs']['requests']}")
        print(f"    Time: {io_result['folded_dvs']['time_ms']:.2f} ms")
        print(f"    Manifest: {io_result['folded_dvs']['manifest_size_kb']:.2f} KB")
        
        print(f"  🎯 I/O reduction: {io_result['io_reduction_pct']:.1f}%")
        print(f"  ⚡ Time reduction: {io_result['time_reduction_pct']:.1f}%")
        print()
        
        results["scenario_b"].append(io_result)
    
    # ========================================================================
    # SCENARIO C: Coalesced Join with Multiple DV Manifests
    # ========================================================================
    print("=" * 80)
    print("SCENARIO C: Coalesced Join with Multiple DV Manifests")
    print("=" * 80)
    print()
    
    test_configs = [
        (10_000, 2),   # 2 DV manifests
        (10_000, 5),   # 5 DV manifests
        (10_000, 10),  # 10 DV manifests
        (25_000, 5),   # Larger manifest
    ]
    
    for num_files, num_manifests in test_configs:
        print(f"--- Testing {num_files:,} files, {num_manifests} DV manifests ---")
        
        coalesce_result = benchmark.benchmark_coalesced_join(num_files, num_manifests)
        
        print(f"  Coalesce time: {coalesce_result['coalesce_time_ms']:.2f} ms")
        print(f"  Resolved DVs: {coalesce_result['resolved_dvs']:,} "
              f"({coalesce_result['resolution_rate_pct']:.1f}%)")
        print()
        
        results["scenario_c"].append(coalesce_result)
    
    # ========================================================================
    # SCENARIO D: Write Overhead for Order-Preserving Manifests
    # ========================================================================
    print("=" * 80)
    print("SCENARIO D: Write Overhead for Order-Preserving Manifests")
    print("=" * 80)
    print()
    
    test_configs = [
        (10_000, 0.1),
        (10_000, 0.3),
        (10_000, 0.5),
        (25_000, 0.3),
    ]
    
    for num_files, dv_ratio in test_configs:
        print(f"--- Testing {num_files:,} files, {dv_ratio*100:.0f}% with DVs ---")
        
        write_result = benchmark.benchmark_write_overhead(num_files, dv_ratio)
        
        print(f"  Unordered write: {write_result['unordered_write']['time_ms']:.2f} ms")
        print(f"  Ordered write: {write_result['ordered_write']['time_ms']:.2f} ms")
        print(f"    NULL entries: {write_result['ordered_write']['null_entries']:,}")
        print(f"  🎯 Overhead: {write_result['overhead_pct']:.1f}%")
        print()
        
        results["scenario_d"].append(write_result)
    
    # ========================================================================
    # Save Results
    # ========================================================================
    output_file = "results/dv_resolution_strategies_results.json"
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print("=" * 80)
    print(f"✅ Results saved to {output_file}")
    print("=" * 80)
    
    return results


if __name__ == "__main__":
    run_all_dv_resolution_benchmarks()
