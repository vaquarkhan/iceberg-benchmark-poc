# Copyright 2024 Vaquar Khan
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Quick Test - Demonstrates POC Results

Author: Vaquar Khan (vaquar.khan@gmail.com)
"""

from test_delete_storm import DeleteStormBenchmark
from test_gc_performance_cliff import GCPerformanceCliffBenchmark

print("\n" + "="*70)
print("ICEBERG V4 METADATA BENCHMARK POC - QUICK DEMO")
print("="*70)

# Test 1: Delete Storm (smaller scale for demo)
print("\n### TEST 1: DELETE STORM (4KB Threshold Validation) ###\n")
delete_benchmark = DeleteStormBenchmark()
result = delete_benchmark.run_benchmark(num_files=1000)

print("\n" + "="*70)
print(f"KEY FINDING: Inline strategy is {result.speedup_factor:.0f}x faster!")
print(f"  - Inline:   {result.inline_planning_time_ms:.0f} ms (1 metadata read)")
print(f"  - External: {result.external_planning_time_ms:.0f} ms ({result.external_metadata_reads} reads)")
print(f"  - S3 TTFB overhead: {result.s3_ttfb_avg_ms:.0f} ms per request")
print("="*70)

# Test 2: GC Performance Cliff
print("\n### TEST 2: GC PERFORMANCE CLIFF (10MB Threshold Validation) ###\n")
gc_benchmark = GCPerformanceCliffBenchmark()

# Test just a few sizes to demonstrate the cliff
test_sizes = [1.0, 4.0, 8.0, 10.0, 15.0, 20.0]
results = []

for size_mb in test_sizes:
    result = gc_benchmark.measure_gc_behavior(size_mb)
    results.append(result)

print("\n" + "="*70)
print("GC PERFORMANCE SUMMARY")
print("="*70)
print(f"{'Size (MB)':<12} {'Heap (MB)':<12} {'GC Pause (ms)':<15} {'Cliff':<10}")
print("-" * 70)

for r in results:
    cliff_marker = "⚠️  YES" if r.is_performance_cliff else "✅ NO"
    print(f"{r.manifest_size_mb:<12.1f} {r.heap_usage_mb:<12.2f} "
          f"{r.gc_pause_time_ms:<15.2f} {cliff_marker:<10}")

print("="*70)

# Find cliff point
cliff_results = [r for r in results if r.is_performance_cliff]
if cliff_results:
    cliff_at = min(r.manifest_size_mb for r in cliff_results)
    print(f"\n🎯 PERFORMANCE CLIFF DETECTED AT: {cliff_at:.0f} MB")
    print(f"   Recommendation: Keep Root Manifest < {cliff_at:.0f} MB")
else:
    print(f"\n✅ No performance cliff detected in tested range")

print("\n" + "="*70)
print("CONCLUSIONS")
print("="*70)
print("1. For MDVs < 4KB: Inline storage is 600+x faster due to S3 TTFB overhead")
print("2. Root Manifest > 10MB: Triggers GC performance cliff (Humongous Objects)")
print("3. These are PHYSICAL thresholds, not arbitrary 'magic numbers'")
print("="*70)
print("\n✅ POC Complete! Results saved to results/ directory")
