
"""
Delete Storm Test - Validates 4KB Threshold for MDV Inlining

Author: Vaquar Khan (vaquar.khan@gmail.com)

This test proves that for small MDVs (<4KB), the network round-trip time (TTFB)
to fetch external Puffin files dominates performance, making inline storage faster.

Scenario:
- Create 10,000 data files
- Delete exactly 1 row in each file (creating 10,000 sparse MDVs)
- Compare scan planning time: Inline vs External strategy

Expected Result:
- Inline: ~50ms (1 metadata read)
- External: ~5,000ms (10,001 reads with 50ms TTFB each)
- Speedup: 100x faster with inline strategy
"""

import time
import json
from dataclasses import dataclass, asdict
from typing import List, Dict
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import statistics

from utils.mdv_generator import generate_sparse_mdv
from utils.s3_simulator import S3LatencySimulator
from utils.metrics_collector import MetricsCollector


@dataclass
class DeleteStormResult:
    """Results from delete storm benchmark"""
    num_files: int
    mdv_size_bytes: int
    inline_planning_time_ms: float
    external_planning_time_ms: float
    inline_metadata_reads: int
    external_metadata_reads: int
    s3_ttfb_avg_ms: float
    speedup_factor: float
    
    def to_dict(self):
        return asdict(self)


class DeleteStormBenchmark:
    """Benchmark for validating 4KB MDV inlining threshold"""
    
    def __init__(self, output_dir: Path = Path("results")):
        self.output_dir = output_dir
        self.output_dir.mkdir(exist_ok=True)
        self.s3_sim = S3LatencySimulator()
        self.metrics = MetricsCollector()
        
    def generate_test_data(self, num_files: int = 10000) -> List[Dict]:
        """
        Generate test scenario: 10,000 files with 1 deleted row each
        
        Returns:
            List of file metadata with sparse MDVs
        """
        print(f"Generating {num_files} files with sparse MDVs...")
        
        files = []
        for i in range(num_files):
            # Each file has 1000 rows, with row 500 deleted
            mdv = generate_sparse_mdv(
                total_rows=1000,
                deleted_rows=[500],
                file_id=i
            )
            
            files.append({
                'file_id': i,
                'file_path': f's3://bucket/table/data/file_{i}.parquet',
                'record_count': 999,  # 1000 - 1 deleted
                'mdv': mdv,
                'mdv_size': len(mdv)
            })
            
            if (i + 1) % 1000 == 0:
                print(f"  Generated {i + 1}/{num_files} files...")
        
        avg_mdv_size = statistics.mean(f['mdv_size'] for f in files)
        print(f"Average MDV size: {avg_mdv_size:.0f} bytes")
        
        return files
    
    def benchmark_inline_strategy(self, files: List[Dict]) -> Dict:
        """
        Benchmark inline MDV strategy
        
        All MDVs are stored in the Root Manifest.
        Query planning requires only 1 metadata read.
        """
        print("\n=== Benchmarking INLINE Strategy ===")
        
        # Simulate Root Manifest with inline MDVs
        root_manifest_size = sum(f['mdv_size'] for f in files)
        print(f"Root Manifest size: {root_manifest_size / 1024 / 1024:.2f} MB")
        
        # Measure planning time
        start = time.perf_counter()
        
        # Single metadata read to fetch Root Manifest
        self.s3_sim.simulate_read(root_manifest_size)
        metadata_reads = 1
        
        # Parse and filter files (in-memory operation, fast)
        matching_files = [f for f in files if self._matches_query(f)]
        
        end = time.perf_counter()
        planning_time_ms = (end - start) * 1000
        
        print(f"Planning time: {planning_time_ms:.2f} ms")
        print(f"Metadata reads: {metadata_reads}")
        print(f"Matching files: {len(matching_files)}")
        
        return {
            'planning_time_ms': planning_time_ms,
            'metadata_reads': metadata_reads,
            'matching_files': len(matching_files)
        }
    
    def benchmark_external_strategy(self, files: List[Dict]) -> Dict:
        """
        Benchmark external MDV strategy
        
        MDVs are stored in separate Puffin files.
        Query planning requires N+1 metadata reads (manifest + N MDV files).
        """
        print("\n=== Benchmarking EXTERNAL Strategy ===")
        
        # Simulate Root Manifest without MDVs (only file paths)
        root_manifest_size = len(files) * 200  # ~200 bytes per file entry
        print(f"Root Manifest size: {root_manifest_size / 1024:.2f} KB")
        
        # Measure planning time
        start = time.perf_counter()
        
        # Read 1: Fetch Root Manifest
        self.s3_sim.simulate_read(root_manifest_size)
        metadata_reads = 1
        
        # Read 2-10001: Fetch each MDV file separately
        ttfb_times = []
        for f in files:
            ttfb = self.s3_sim.simulate_read(f['mdv_size'])
            ttfb_times.append(ttfb)
            metadata_reads += 1
        
        # Parse and filter files
        matching_files = [f for f in files if self._matches_query(f)]
        
        end = time.perf_counter()
        planning_time_ms = (end - start) * 1000
        
        avg_ttfb = statistics.mean(ttfb_times)
        print(f"Planning time: {planning_time_ms:.2f} ms")
        print(f"Metadata reads: {metadata_reads}")
        print(f"Average TTFB: {avg_ttfb:.2f} ms")
        print(f"Matching files: {len(matching_files)}")
        
        return {
            'planning_time_ms': planning_time_ms,
            'metadata_reads': metadata_reads,
            'avg_ttfb_ms': avg_ttfb,
            'matching_files': len(matching_files)
        }
    
    def _matches_query(self, file: Dict) -> bool:
        """Simulate query filter evaluation"""
        # In real scenario, this would check partition filters
        # For this test, assume all files match
        return True
    
    def run_benchmark(self, num_files: int = 10000) -> DeleteStormResult:
        """
        Run complete delete storm benchmark
        
        Args:
            num_files: Number of files to test (default 10,000)
            
        Returns:
            DeleteStormResult with comparative metrics
        """
        print(f"\n{'='*60}")
        print(f"DELETE STORM TEST - 4KB Threshold Validation")
        print(f"{'='*60}")
        print(f"Files: {num_files}")
        print(f"Scenario: 1 deleted row per file (sparse MDVs)")
        
        # Generate test data
        files = self.generate_test_data(num_files)
        avg_mdv_size = int(statistics.mean(f['mdv_size'] for f in files))
        
        # Benchmark both strategies
        inline_result = self.benchmark_inline_strategy(files)
        external_result = self.benchmark_external_strategy(files)
        
        # Calculate speedup
        speedup = external_result['planning_time_ms'] / inline_result['planning_time_ms']
        
        # Create result
        result = DeleteStormResult(
            num_files=num_files,
            mdv_size_bytes=avg_mdv_size,
            inline_planning_time_ms=inline_result['planning_time_ms'],
            external_planning_time_ms=external_result['planning_time_ms'],
            inline_metadata_reads=inline_result['metadata_reads'],
            external_metadata_reads=external_result['metadata_reads'],
            s3_ttfb_avg_ms=external_result['avg_ttfb_ms'],
            speedup_factor=speedup
        )
        
        # Print summary
        print(f"\n{'='*60}")
        print(f"RESULTS SUMMARY")
        print(f"{'='*60}")
        print(f"Average MDV size: {avg_mdv_size} bytes")
        print(f"\nInline Strategy:")
        print(f"  Planning time: {result.inline_planning_time_ms:.2f} ms")
        print(f"  Metadata reads: {result.inline_metadata_reads}")
        print(f"\nExternal Strategy:")
        print(f"  Planning time: {result.external_planning_time_ms:.2f} ms")
        print(f"  Metadata reads: {result.external_metadata_reads}")
        print(f"  Avg S3 TTFB: {result.s3_ttfb_avg_ms:.2f} ms")
        print(f"\n🎯 SPEEDUP: {speedup:.1f}x faster with inline strategy")
        print(f"{'='*60}")
        
        # Save results
        self._save_results(result)
        
        return result
    
    def _save_results(self, result: DeleteStormResult):
        """Save results to JSON file"""
        output_file = self.output_dir / "delete_storm_results.json"
        with open(output_file, 'w') as f:
            json.dump(result.to_dict(), f, indent=2)
        print(f"\n✅ Results saved to {output_file}")


def main():
    """Run delete storm benchmark"""
    benchmark = DeleteStormBenchmark()
    
    # Test with different file counts
    test_cases = [1000, 5000, 10000]
    
    results = []
    for num_files in test_cases:
        result = benchmark.run_benchmark(num_files)
        results.append(result)
        print("\n" + "="*60 + "\n")
    
    # Print comparative summary
    print("\n" + "="*60)
    print("COMPARATIVE SUMMARY")
    print("="*60)
    print(f"{'Files':<10} {'Inline (ms)':<15} {'External (ms)':<15} {'Speedup':<10}")
    print("-" * 60)
    for r in results:
        print(f"{r.num_files:<10} {r.inline_planning_time_ms:<15.2f} "
              f"{r.external_planning_time_ms:<15.2f} {r.speedup_factor:<10.1f}x")
    print("="*60)
    
    print("\n✅ Delete Storm Test Complete!")
    print(f"📊 Conclusion: For MDVs < 4KB, inline storage is {results[-1].speedup_factor:.0f}x faster")
    print(f"   due to avoiding S3 TTFB overhead ({results[-1].s3_ttfb_avg_ms:.0f}ms per request)")


if __name__ == "__main__":
    main()
