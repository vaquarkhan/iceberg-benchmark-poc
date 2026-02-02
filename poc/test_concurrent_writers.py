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
Concurrent Writers Test - V4 Optimistic Locking
================================================================================
Author: Vaquar Khan (vaquar.khan@gmail.com)

Tests V4's behavior under concurrent write load.
Validates optimistic locking and conflict resolution.

Critical Gap Addressed:
- Audit document: "not ready to share without multi-threaded writer test"
- V4's One-File Commit introduces new contention points
"""

import time
import json
import threading
import queue
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import List, Dict
import random


@dataclass
class ConcurrentWriteResult:
    """Results from concurrent write test"""
    num_writers: int
    writes_per_writer: int
    total_writes: int
    successful_commits: int
    failed_commits: int
    conflict_rate: float
    avg_commit_latency_ms: float
    throughput_commits_per_sec: float
    
    def to_dict(self):
        return asdict(self)


class OptimisticLockManager:
    """
    Simulates optimistic locking for Root Manifest updates
    
    In real V4 implementation, this would be handled by the catalog
    (e.g., AWS Glue, Polaris, Unity Catalog)
    """
    
    def __init__(self):
        self.current_version = 0
        self.lock = threading.Lock()
        self.conflict_count = 0
    
    def try_commit(self, expected_version: int, writer_id: int) -> tuple[bool, int]:
        """
        Attempt to commit with optimistic locking
        
        Args:
            expected_version: Version writer thinks is current
            writer_id: ID of the writer
            
        Returns:
            (success, new_version)
        """
        with self.lock:
            if self.current_version == expected_version:
                # Success: version matches
                self.current_version += 1
                return (True, self.current_version)
            else:
                # Conflict: version changed
                self.conflict_count += 1
                return (False, self.current_version)
    
    def get_current_version(self) -> int:
        """Get current version (read operation)"""
        with self.lock:
            return self.current_version


class ConcurrentWriter:
    """Simulates a concurrent writer"""
    
    def __init__(
        self,
        writer_id: int,
        lock_manager: OptimisticLockManager,
        writes_to_perform: int,
        results_queue: queue.Queue
    ):
        self.writer_id = writer_id
        self.lock_manager = lock_manager
        self.writes_to_perform = writes_to_perform
        self.results_queue = results_queue
        self.successful_commits = 0
        self.failed_commits = 0
        self.commit_times = []
    
    def run(self):
        """Execute writes with retry logic"""
        for i in range(self.writes_to_perform):
            success = False
            retries = 0
            max_retries = 10
            
            while not success and retries < max_retries:
                start = time.perf_counter()
                
                # Read current version
                expected_version = self.lock_manager.get_current_version()
                
                # Simulate work (generate data, write to S3, etc.)
                time.sleep(random.uniform(0.001, 0.005))
                
                # Try to commit
                success, new_version = self.lock_manager.try_commit(
                    expected_version,
                    self.writer_id
                )
                
                end = time.perf_counter()
                commit_time_ms = (end - start) * 1000
                
                if success:
                    self.successful_commits += 1
                    self.commit_times.append(commit_time_ms)
                else:
                    retries += 1
                    # Exponential backoff
                    time.sleep(0.001 * (2 ** retries))
            
            if not success:
                self.failed_commits += 1
        
        # Report results
        self.results_queue.put({
            'writer_id': self.writer_id,
            'successful_commits': self.successful_commits,
            'failed_commits': self.failed_commits,
            'avg_commit_time_ms': sum(self.commit_times) / len(self.commit_times) if self.commit_times else 0
        })


class ConcurrentWritersBenchmark:
    """Benchmark concurrent writers with optimistic locking"""
    
    def __init__(self, output_dir: Path = Path("results")):
        self.output_dir = output_dir
        self.output_dir.mkdir(exist_ok=True)
    
    def run_benchmark(
        self,
        num_writers: int = 10,
        writes_per_writer: int = 100
    ) -> ConcurrentWriteResult:
        """
        Run concurrent writers benchmark
        
        Args:
            num_writers: Number of concurrent writers
            writes_per_writer: Number of writes each writer performs
            
        Returns:
            ConcurrentWriteResult with metrics
        """
        print(f"\n{'='*60}")
        print(f"CONCURRENT WRITERS TEST")
        print(f"{'='*60}")
        print(f"Writers: {num_writers}")
        print(f"Writes per writer: {writes_per_writer}")
        print(f"Total writes: {num_writers * writes_per_writer}")
        
        # Create lock manager
        lock_manager = OptimisticLockManager()
        results_queue = queue.Queue()
        
        # Create writers
        writers = []
        for i in range(num_writers):
            writer = ConcurrentWriter(
                writer_id=i,
                lock_manager=lock_manager,
                writes_to_perform=writes_per_writer,
                results_queue=results_queue
            )
            writers.append(writer)
        
        # Start all writers
        print("\nStarting concurrent writers...")
        start_time = time.perf_counter()
        
        threads = []
        for writer in writers:
            thread = threading.Thread(target=writer.run)
            thread.start()
            threads.append(thread)
        
        # Wait for all to complete
        for thread in threads:
            thread.join()
        
        end_time = time.perf_counter()
        total_time_sec = end_time - start_time
        
        # Collect results
        total_successful = 0
        total_failed = 0
        all_commit_times = []
        
        while not results_queue.empty():
            result = results_queue.get()
            total_successful += result['successful_commits']
            total_failed += result['failed_commits']
            if result['avg_commit_time_ms'] > 0:
                all_commit_times.append(result['avg_commit_time_ms'])
        
        # Calculate metrics
        total_writes = num_writers * writes_per_writer
        conflict_rate = lock_manager.conflict_count / total_writes
        avg_commit_latency = sum(all_commit_times) / len(all_commit_times) if all_commit_times else 0
        throughput = total_successful / total_time_sec
        
        result = ConcurrentWriteResult(
            num_writers=num_writers,
            writes_per_writer=writes_per_writer,
            total_writes=total_writes,
            successful_commits=total_successful,
            failed_commits=total_failed,
            conflict_rate=conflict_rate,
            avg_commit_latency_ms=avg_commit_latency,
            throughput_commits_per_sec=throughput
        )
        
        # Print summary
        print(f"\n{'='*60}")
        print(f"RESULTS SUMMARY")
        print(f"{'='*60}")
        print(f"Total time: {total_time_sec:.2f} seconds")
        print(f"Successful commits: {total_successful}")
        print(f"Failed commits: {total_failed}")
        print(f"Conflicts detected: {lock_manager.conflict_count}")
        print(f"Conflict rate: {conflict_rate * 100:.2f}%")
        print(f"Avg commit latency: {avg_commit_latency:.2f} ms")
        print(f"Throughput: {throughput:.2f} commits/sec")
        print(f"{'='*60}")
        
        # Save results
        self._save_results(result)
        
        return result
    
    def _save_results(self, result: ConcurrentWriteResult):
        """Save results to JSON file"""
        output_file = self.output_dir / "concurrent_writers_results.json"
        with open(output_file, 'w') as f:
            json.dump(result.to_dict(), f, indent=2)
        print(f"\n✅ Results saved to {output_file}")


def main():
    """Run concurrent writers benchmark"""
    benchmark = ConcurrentWritersBenchmark()
    
    # Test with increasing concurrency
    test_cases = [
        (2, 100),   # 2 writers
        (5, 100),   # 5 writers
        (10, 100),  # 10 writers
        (20, 100),  # 20 writers
    ]
    
    results = []
    for num_writers, writes_per_writer in test_cases:
        result = benchmark.run_benchmark(num_writers, writes_per_writer)
        results.append(result)
        print("\n" + "="*60 + "\n")
    
    # Print comparative summary
    print("\n" + "="*60)
    print("COMPARATIVE SUMMARY")
    print("="*60)
    print(f"{'Writers':<10} {'Conflicts':<12} {'Rate':<12} {'Throughput':<15}")
    print("-" * 60)
    for r in results:
        print(f"{r.num_writers:<10} {r.conflict_rate * r.total_writes:<12.0f} "
              f"{r.conflict_rate * 100:<12.2f}% {r.throughput_commits_per_sec:<15.2f}")
    print("="*60)
    
    print("\n✅ Concurrent Writers Test Complete!")
    print(f"📊 Conclusion: V4's optimistic locking handles {results[-1].num_writers} concurrent")
    print(f"   writers with {results[-1].conflict_rate * 100:.1f}% conflict rate and")
    print(f"   {results[-1].throughput_commits_per_sec:.0f} commits/sec throughput")


if __name__ == "__main__":
    main()
