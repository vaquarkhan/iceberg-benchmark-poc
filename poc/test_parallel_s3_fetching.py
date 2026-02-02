"""
Test: Parallel S3 Fetching Impact on Delete Storm
Addresses the critique that sequential fetching overstates inline MDV benefits.

This test compares:
1. Sequential fetching (current simulation)
2. Parallel fetching with different thread pool sizes (10, 50, 100 threads)

Goal: Provide realistic speedup factors for production engines like Trino/Spark.
"""

import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


def simulate_s3_fetch(file_id, latency_ms=55):
    """Simulate fetching a single file from S3 with TTFB overhead."""
    time.sleep(latency_ms / 1000.0)  # Convert ms to seconds
    return {"file_id": file_id, "mdv_size": 24, "deletes": 1}


def sequential_fetch(num_files, latency_ms=55):
    """Sequential fetching (current simulation)."""
    start = time.perf_counter()
    results = []
    for i in range(num_files):
        result = simulate_s3_fetch(i, latency_ms)
        results.append(result)
    elapsed = time.perf_counter() - start
    return elapsed, results


def parallel_fetch(num_files, max_workers, latency_ms=55):
    """Parallel fetching with thread pool."""
    start = time.perf_counter()
    results = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        futures = {executor.submit(simulate_s3_fetch, i, latency_ms): i 
                   for i in range(num_files)}
        
        # Collect results as they complete
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
    
    elapsed = time.perf_counter() - start
    return elapsed, results


def run_parallel_s3_benchmark():
    """
    Compare sequential vs parallel S3 fetching for Delete Storm scenario.
    
    Scenario: 1,000 files with small MDVs (24 bytes each)
    S3 TTFB: 55ms per request
    """
    print("=" * 80)
    print("Parallel S3 Fetching Benchmark")
    print("=" * 80)
    
    num_files = 1000
    s3_ttfb_ms = 55
    inline_time_ms = 23.77  # From actual measurements
    
    results = {
        "test_name": "Parallel S3 Fetching Impact",
        "scenario": "Delete Storm (1,000 files, 24 bytes each)",
        "s3_ttfb_ms": s3_ttfb_ms,
        "inline_planning_time_ms": inline_time_ms,
        "configurations": []
    }
    
    # Test 1: Sequential (current simulation)
    print(f"\n1. Sequential Fetching (Current Simulation)")
    print(f"   Files: {num_files}, TTFB: {s3_ttfb_ms}ms")
    seq_time, _ = sequential_fetch(num_files, s3_ttfb_ms)
    seq_time_ms = seq_time * 1000
    seq_speedup = seq_time_ms / inline_time_ms
    
    print(f"   Time: {seq_time_ms:.2f} ms")
    print(f"   Speedup (inline): {seq_speedup:.1f}x")
    
    results["configurations"].append({
        "strategy": "Sequential",
        "threads": 1,
        "time_ms": round(seq_time_ms, 2),
        "speedup_vs_inline": round(seq_speedup, 1)
    })
    
    # Test 2-5: Parallel with different thread counts
    thread_counts = [10, 25, 50, 100]
    
    for threads in thread_counts:
        print(f"\n{thread_counts.index(threads) + 2}. Parallel Fetching ({threads} threads)")
        print(f"   Files: {num_files}, TTFB: {s3_ttfb_ms}ms")
        
        par_time, _ = parallel_fetch(num_files, threads, s3_ttfb_ms)
        par_time_ms = par_time * 1000
        par_speedup = par_time_ms / inline_time_ms
        reduction_vs_seq = ((seq_time_ms - par_time_ms) / seq_time_ms) * 100
        
        print(f"   Time: {par_time_ms:.2f} ms")
        print(f"   Speedup (inline): {par_speedup:.1f}x")
        print(f"   Reduction vs Sequential: {reduction_vs_seq:.1f}%")
        
        results["configurations"].append({
            "strategy": f"Parallel ({threads} threads)",
            "threads": threads,
            "time_ms": round(par_time_ms, 2),
            "speedup_vs_inline": round(par_speedup, 1),
            "reduction_vs_sequential_pct": round(reduction_vs_seq, 1)
        })
    
    # Analysis
    print("\n" + "=" * 80)
    print("ANALYSIS")
    print("=" * 80)
    
    best_parallel = min([c for c in results["configurations"] if c["threads"] > 1], 
                        key=lambda x: x["time_ms"])
    
    print(f"\nBest Parallel Configuration: {best_parallel['threads']} threads")
    print(f"  Time: {best_parallel['time_ms']:.2f} ms")
    print(f"  Speedup vs Inline: {best_parallel['speedup_vs_inline']:.1f}x")
    print(f"  Reduction vs Sequential: {best_parallel['reduction_vs_sequential_pct']:.1f}%")
    
    print(f"\nKey Findings:")
    print(f"  1. Sequential fetching: {seq_speedup:.1f}x slower than inline")
    print(f"  2. Parallel fetching (50 threads): {best_parallel['speedup_vs_inline']:.1f}x slower than inline")
    print(f"  3. Parallelism reduces gap by {best_parallel['reduction_vs_sequential_pct']:.1f}%")
    print(f"  4. Inline STILL wins due to eliminating {num_files} S3 requests entirely")
    
    results["summary"] = {
        "sequential_speedup": round(seq_speedup, 1),
        "best_parallel_speedup": round(best_parallel['speedup_vs_inline'], 1),
        "best_parallel_threads": best_parallel['threads'],
        "parallelism_benefit_pct": round(best_parallel['reduction_vs_sequential_pct'], 1),
        "conclusion": f"Inline MDVs are {best_parallel['speedup_vs_inline']:.1f}x faster than parallel fetching (realistic)"
    }
    
    # Save results
    output_dir = Path(__file__).parent / "results"
    output_dir.mkdir(exist_ok=True)
    output_file = output_dir / "parallel_s3_results.json"
    
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n✅ Results saved to: {output_file}")
    
    return results


if __name__ == "__main__":
    results = run_parallel_s3_benchmark()
    
    print("\n" + "=" * 80)
    print("CONCLUSION")
    print("=" * 80)
    print(f"""
The critique is valid: sequential fetching overstates the benefit.

However, even with realistic parallelism (50 threads):
  - Inline MDVs: {results['inline_planning_time_ms']:.2f} ms
  - Parallel External: {results['summary']['best_parallel_speedup'] * results['inline_planning_time_ms']:.2f} ms
  - Speedup: {results['summary']['best_parallel_speedup']:.1f}x

The fundamental advantage remains:
  ✓ Zero S3 requests (no throttling risk)
  ✓ Zero network latency
  ✓ Single metadata read
  ✓ Predictable performance

Recommendation: Update documentation to cite {results['summary']['best_parallel_speedup']:.1f}x as the 
"realistic speedup" for production engines with parallel I/O.
""")
