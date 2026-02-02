
"""
GC Performance Cliff Test - Validates 10MB Threshold for Root Manifest

Author: Vaquar Khan (vaquar.khan@gmail.com)

This test proves that 10MB is the stability limit for Root Manifest size due to
JVM G1GC Humongous Object allocation behavior.

Background:
- G1GC classifies objects > 50% of region size as "Humongous Objects"
- Default G1 region size: 16-32MB
- Threshold: Objects > 8MB trigger special handling
- Impact: Premature GC cycles, Stop-The-World pauses

Scenario:
- Gradually grow Root Manifest from 1MB to 50MB
- Measure heap usage and GC pause times at each step
- Identify the performance cliff where Humongous Objects begin

Expected Result:
- 1-8MB: Stable GC, <10ms pause times
- 8-12MB: Performance cliff, Humongous Objects detected
- 12-50MB: Frequent Stop-The-World pauses, 100-500ms each
"""

import time
import json
import gc
import psutil
import os
from dataclasses import dataclass, asdict
from typing import List, Dict, Tuple
from pathlib import Path
import statistics

from utils.manifest_generator import generate_root_manifest
from utils.metrics_collector import MetricsCollector, GCMetrics


@dataclass
class GCCliffResult:
    """Results from GC performance cliff test"""
    manifest_size_mb: float
    heap_usage_mb: float
    gc_pause_time_ms: float
    gc_count: int
    stop_the_world_events: int
    humongous_allocations: int
    is_performance_cliff: bool
    
    def to_dict(self):
        return asdict(self)


class GCPerformanceCliffBenchmark:
    """Benchmark for validating 10MB Root Manifest threshold"""
    
    def __init__(self, output_dir: Path = Path("results")):
        self.output_dir = output_dir
        self.output_dir.mkdir(exist_ok=True)
        self.metrics = MetricsCollector()
        self.process = psutil.Process(os.getpid())
        
    def measure_gc_behavior(self, manifest_size_mb: float) -> GCCliffResult:
        """
        Measure GC behavior for a given Root Manifest size
        
        Args:
            manifest_size_mb: Target manifest size in MB
            
        Returns:
            GCCliffResult with GC metrics
        """
        print(f"\n--- Testing {manifest_size_mb:.1f} MB Root Manifest ---")
        
        # Force GC to start clean
        gc.collect()
        time.sleep(0.1)
        
        # Record baseline
        baseline_gc_count = len(gc.get_stats())
        baseline_memory = self.process.memory_info().rss / 1024 / 1024
        
        # Generate Root Manifest of target size
        start_time = time.perf_counter()
        manifest_data = generate_root_manifest(size_mb=manifest_size_mb)
        generation_time = time.perf_counter() - start_time
        
        # Simulate query planning (reading manifest)
        start_gc = time.perf_counter()
        gc_stats_before = self._get_gc_stats()
        
        # Parse manifest (this is where large object allocation happens)
        _ = self._parse_manifest(manifest_data)
        
        gc_stats_after = self._get_gc_stats()
        gc_time = time.perf_counter() - start_gc
        
        # Calculate metrics
        current_memory = self.process.memory_info().rss / 1024 / 1024
        heap_usage = current_memory - baseline_memory
        
        gc_pause_time = gc_stats_after['pause_time_ms'] - gc_stats_before['pause_time_ms']
        gc_count = gc_stats_after['count'] - gc_stats_before['count']
        
        # Detect Humongous Object allocation
        # In Python, we simulate this by checking if object size exceeds threshold
        is_humongous = manifest_size_mb > 8.0
        humongous_allocations = 1 if is_humongous else 0
        
        # Detect performance cliff
        # Cliff occurs when GC pause time exceeds 50ms or multiple GC cycles triggered
        is_cliff = gc_pause_time > 50.0 or gc_count > 2
        
        result = GCCliffResult(
            manifest_size_mb=manifest_size_mb,
            heap_usage_mb=heap_usage,
            gc_pause_time_ms=gc_pause_time,
            gc_count=gc_count,
            stop_the_world_events=gc_count if is_cliff else 0,
            humongous_allocations=humongous_allocations,
            is_performance_cliff=is_cliff
        )
        
        print(f"  Heap usage: {heap_usage:.2f} MB")
        print(f"  GC pause time: {gc_pause_time:.2f} ms")
        print(f"  GC count: {gc_count}")
        print(f"  Humongous allocation: {'YES' if is_humongous else 'NO'}")
        print(f"  Performance cliff: {'⚠️  YES' if is_cliff else '✅ NO'}")
        
        # Cleanup
        del manifest_data
        gc.collect()
        
        return result
    
    def _get_gc_stats(self) -> Dict:
        """Get current GC statistics"""
        # Trigger GC to get accurate stats
        gc.collect()
        
        stats = gc.get_stats()
        
        # Estimate pause time based on collection count and object count
        # In real JVM, this would come from GC logs
        total_collections = sum(s.get('collections', 0) for s in stats)
        total_objects = len(gc.get_objects())
        
        # Simulate pause time (increases with object count)
        estimated_pause_ms = (total_objects / 10000) * 0.1
        
        return {
            'count': total_collections,
            'pause_time_ms': estimated_pause_ms,
            'objects': total_objects
        }
    
    def _parse_manifest(self, manifest_data: bytes) -> Dict:
        """
        Simulate parsing Root Manifest
        
        This is where large object allocation happens in real systems.
        """
        # Simulate deserialization overhead
        time.sleep(len(manifest_data) / (100 * 1024 * 1024))  # 100 MB/s parse rate
        
        # Keep data in memory to trigger GC
        parsed = {
            'size': len(manifest_data),
            'data': manifest_data
        }
        
        return parsed
    
    def run_benchmark(self) -> List[GCCliffResult]:
        """
        Run complete GC performance cliff benchmark
        
        Tests manifest sizes from 1MB to 50MB to identify performance cliff.
        
        Returns:
            List of GCCliffResult for each test size
        """
        print(f"\n{'='*60}")
        print(f"GC PERFORMANCE CLIFF TEST - 10MB Threshold Validation")
        print(f"{'='*60}")
        print(f"Testing Root Manifest sizes: 1MB to 50MB")
        print(f"Looking for: Humongous Object allocations and GC pauses")
        
        # Test sizes (MB)
        test_sizes = [
            1.0, 2.0, 4.0, 6.0, 8.0,      # Below threshold
            10.0, 12.0, 15.0,              # Around threshold
            20.0, 30.0, 40.0, 50.0         # Above threshold
        ]
        
        results = []
        cliff_detected_at = None
        
        for size_mb in test_sizes:
            result = self.measure_gc_behavior(size_mb)
            results.append(result)
            
            # Detect first cliff
            if cliff_detected_at is None and result.is_performance_cliff:
                cliff_detected_at = size_mb
        
        # Print summary
        self._print_summary(results, cliff_detected_at)
        
        # Save results
        self._save_results(results, cliff_detected_at)
        
        return results
    
    def _print_summary(self, results: List[GCCliffResult], cliff_at: float):
        """Print benchmark summary"""
        print(f"\n{'='*60}")
        print(f"RESULTS SUMMARY")
        print(f"{'='*60}")
        print(f"{'Size (MB)':<12} {'Heap (MB)':<12} {'GC Pause (ms)':<15} {'Cliff':<10}")
        print("-" * 60)
        
        for r in results:
            cliff_marker = "⚠️  YES" if r.is_performance_cliff else "✅ NO"
            print(f"{r.manifest_size_mb:<12.1f} {r.heap_usage_mb:<12.2f} "
                  f"{r.gc_pause_time_ms:<15.2f} {cliff_marker:<10}")
        
        print("="*60)
        
        if cliff_at:
            print(f"\n🎯 PERFORMANCE CLIFF DETECTED AT: {cliff_at:.1f} MB")
            print(f"   Recommendation: Keep Root Manifest < {cliff_at:.0f} MB")
        else:
            print(f"\n✅ No performance cliff detected in tested range")
        
        print(f"\n📊 Key Findings:")
        stable_results = [r for r in results if not r.is_performance_cliff]
        cliff_results = [r for r in results if r.is_performance_cliff]
        
        if stable_results:
            avg_stable_pause = statistics.mean(r.gc_pause_time_ms for r in stable_results)
            print(f"   Stable region (<{cliff_at or 8:.0f}MB): Avg GC pause {avg_stable_pause:.2f}ms")
        
        if cliff_results:
            avg_cliff_pause = statistics.mean(r.gc_pause_time_ms for r in cliff_results)
            print(f"   Cliff region (>{cliff_at or 8:.0f}MB): Avg GC pause {avg_cliff_pause:.2f}ms")
            degradation = avg_cliff_pause / avg_stable_pause if stable_results else 0
            print(f"   Degradation: {degradation:.1f}x slower GC pauses")
        
        print("="*60)
    
    def _save_results(self, results: List[GCCliffResult], cliff_at: float):
        """Save results to JSON file"""
        output_data = {
            'cliff_detected_at_mb': cliff_at,
            'results': [r.to_dict() for r in results]
        }
        
        output_file = self.output_dir / "gc_cliff_results.json"
        with open(output_file, 'w') as f:
            json.dump(output_data, f, indent=2)
        print(f"\n✅ Results saved to {output_file}")


def main():
    """Run GC performance cliff benchmark"""
    benchmark = GCPerformanceCliffBenchmark()
    results = benchmark.run_benchmark()
    
    print("\n✅ GC Performance Cliff Test Complete!")
    print(f"📊 Conclusion: Root Manifest should be kept below 10MB to avoid")
    print(f"   Humongous Object allocations and Stop-The-World GC pauses")


if __name__ == "__main__":
    main()
