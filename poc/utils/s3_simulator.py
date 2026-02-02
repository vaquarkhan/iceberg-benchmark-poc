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
S3 Latency Simulator
================================================================================
Author: Vaquar Khan (vaquar.khan@gmail.com)
Purpose: Simulate S3 read latency including TTFB (Time To First Byte)
"""

import time
import random
from typing import Dict
from dataclasses import dataclass


@dataclass
class S3LatencyProfile:
    """S3 latency characteristics"""
    ttfb_min_ms: float = 20.0      # Minimum TTFB
    ttfb_avg_ms: float = 50.0      # Average TTFB
    ttfb_max_ms: float = 150.0     # Maximum TTFB (p99)
    throughput_mbps: float = 100.0  # Download throughput in MB/s
    
    def sample_ttfb(self) -> float:
        """Sample TTFB from distribution"""
        # Use log-normal distribution to simulate real S3 latency
        # Most requests are fast, but occasional slow requests
        mean = self.ttfb_avg_ms
        std = (self.ttfb_max_ms - self.ttfb_min_ms) / 4
        
        ttfb = random.gauss(mean, std)
        return max(self.ttfb_min_ms, min(self.ttfb_max_ms, ttfb))


class S3LatencySimulator:
    """Simulates S3 read operations with realistic latency"""
    
    def __init__(self, profile: S3LatencyProfile = None):
        self.profile = profile or S3LatencyProfile()
        self.stats = {
            'total_reads': 0,
            'total_bytes': 0,
            'total_time_ms': 0.0,
            'ttfb_samples': []
        }
    
    def simulate_read(self, size_bytes: int) -> float:
        """
        Simulate reading data from S3
        
        Args:
            size_bytes: Size of data to read
            
        Returns:
            Total time in milliseconds (TTFB + transfer time)
        """
        # Time To First Byte
        ttfb_ms = self.profile.sample_ttfb()
        
        # Transfer time based on throughput
        transfer_time_ms = (size_bytes / (1024 * 1024)) / self.profile.throughput_mbps * 1000
        
        # Total time
        total_time_ms = ttfb_ms + transfer_time_ms
        
        # Simulate actual delay
        time.sleep(total_time_ms / 1000)
        
        # Update stats
        self.stats['total_reads'] += 1
        self.stats['total_bytes'] += size_bytes
        self.stats['total_time_ms'] += total_time_ms
        self.stats['ttfb_samples'].append(ttfb_ms)
        
        return ttfb_ms
    
    def simulate_batch_read(self, sizes: list) -> Dict:
        """
        Simulate reading multiple objects
        
        Args:
            sizes: List of object sizes in bytes
            
        Returns:
            Dictionary with timing statistics
        """
        start = time.perf_counter()
        
        ttfb_times = []
        total_times = []
        
        for size in sizes:
            ttfb = self.simulate_read(size)
            ttfb_times.append(ttfb)
        
        end = time.perf_counter()
        wall_time_ms = (end - start) * 1000
        
        return {
            'count': len(sizes),
            'total_bytes': sum(sizes),
            'wall_time_ms': wall_time_ms,
            'avg_ttfb_ms': sum(ttfb_times) / len(ttfb_times),
            'min_ttfb_ms': min(ttfb_times),
            'max_ttfb_ms': max(ttfb_times)
        }
    
    def get_stats(self) -> Dict:
        """Get cumulative statistics"""
        if self.stats['total_reads'] == 0:
            return self.stats
        
        avg_ttfb = sum(self.stats['ttfb_samples']) / len(self.stats['ttfb_samples'])
        
        return {
            **self.stats,
            'avg_ttfb_ms': avg_ttfb,
            'avg_bytes_per_read': self.stats['total_bytes'] / self.stats['total_reads'],
            'avg_time_per_read_ms': self.stats['total_time_ms'] / self.stats['total_reads']
        }
    
    def reset_stats(self):
        """Reset statistics"""
        self.stats = {
            'total_reads': 0,
            'total_bytes': 0,
            'total_time_ms': 0.0,
            'ttfb_samples': []
        }


def calculate_inline_vs_external_cost(
    num_files: int,
    avg_mdv_size: int,
    s3_profile: S3LatencyProfile = None
) -> Dict:
    """
    Calculate cost comparison: inline vs external MDV storage
    
    Args:
        num_files: Number of files with MDVs
        avg_mdv_size: Average MDV size in bytes
        s3_profile: S3 latency profile
        
    Returns:
        Cost comparison dictionary
    """
    profile = s3_profile or S3LatencyProfile()
    
    # Inline strategy: Single read of large manifest
    inline_manifest_size = num_files * (200 + avg_mdv_size)  # 200 bytes base + MDV
    inline_ttfb = profile.ttfb_avg_ms
    inline_transfer = (inline_manifest_size / (1024 * 1024)) / profile.throughput_mbps * 1000
    inline_total_ms = inline_ttfb + inline_transfer
    
    # External strategy: N+1 reads (manifest + N MDV files)
    external_manifest_size = num_files * 200  # Base entries only
    external_manifest_time = profile.ttfb_avg_ms + (external_manifest_size / (1024 * 1024)) / profile.throughput_mbps * 1000
    
    # Each MDV file requires separate TTFB
    external_mdv_time = num_files * (profile.ttfb_avg_ms + (avg_mdv_size / (1024 * 1024)) / profile.throughput_mbps * 1000)
    
    external_total_ms = external_manifest_time + external_mdv_time
    
    return {
        'num_files': num_files,
        'avg_mdv_size': avg_mdv_size,
        'inline': {
            'manifest_size_mb': inline_manifest_size / (1024 * 1024),
            'reads': 1,
            'total_time_ms': inline_total_ms,
            'ttfb_overhead_ms': inline_ttfb
        },
        'external': {
            'manifest_size_mb': external_manifest_size / (1024 * 1024),
            'reads': num_files + 1,
            'total_time_ms': external_total_ms,
            'ttfb_overhead_ms': profile.ttfb_avg_ms * (num_files + 1)
        },
        'speedup': external_total_ms / inline_total_ms
    }


if __name__ == "__main__":
    # Test S3 simulator
    print("Testing S3 Latency Simulator...")
    
    sim = S3LatencySimulator()
    
    # Simulate reading 10 small files (typical MDV size)
    print("\nSimulating 10 small file reads (2KB each):")
    result = sim.simulate_batch_read([2048] * 10)
    print(f"  Total time: {result['wall_time_ms']:.2f} ms")
    print(f"  Avg TTFB: {result['avg_ttfb_ms']:.2f} ms")
    print(f"  TTFB overhead: {result['avg_ttfb_ms'] * 10:.2f} ms")
    
    # Compare inline vs external
    print("\n" + "="*60)
    print("Cost Analysis: Inline vs External MDV Storage")
    print("="*60)
    
    for num_files in [100, 1000, 10000]:
        cost = calculate_inline_vs_external_cost(num_files, avg_mdv_size=2048)
        print(f"\n{num_files} files with 2KB MDVs:")
        print(f"  Inline:   {cost['inline']['total_time_ms']:.0f} ms ({cost['inline']['reads']} reads)")
        print(f"  External: {cost['external']['total_time_ms']:.0f} ms ({cost['external']['reads']} reads)")
        print(f"  Speedup:  {cost['speedup']:.1f}x faster with inline")
    
    print("\n✅ Conclusion: For small MDVs, TTFB dominates and inline is much faster")
