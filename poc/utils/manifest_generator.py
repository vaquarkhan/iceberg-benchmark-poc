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
Root Manifest Generator
================================================================================
Author: Vaquar Khan (vaquar.khan@gmail.com)
Purpose: Generate Root Manifest files of various sizes for GC testing
"""

import struct
import random
from typing import List, Dict
from datetime import datetime, timedelta


def generate_root_manifest(size_mb: float, num_files: int = None) -> bytes:
    """
    Generate a Root Manifest of specified size
    
    Args:
        size_mb: Target size in megabytes
        num_files: Number of file entries (auto-calculated if None)
        
    Returns:
        Root Manifest as bytes
    """
    target_size = int(size_mb * 1024 * 1024)
    
    # Estimate bytes per file entry
    # Typical entry: ~200 bytes (path, stats, partition info)
    bytes_per_entry = 200
    
    if num_files is None:
        num_files = target_size // bytes_per_entry
    
    manifest_data = bytearray()
    
    # Header
    manifest_data.extend(b'ICEBERG_V4_MANIFEST')
    manifest_data.extend(struct.pack('<I', 1))  # Version
    manifest_data.extend(struct.pack('<Q', num_files))  # File count
    
    # Generate file entries
    for i in range(num_files):
        entry = generate_file_entry(i)
        manifest_data.extend(entry)
        
        # Check if we've reached target size
        if len(manifest_data) >= target_size:
            break
    
    # Pad to exact size if needed
    if len(manifest_data) < target_size:
        padding = target_size - len(manifest_data)
        manifest_data.extend(b'\x00' * padding)
    
    return bytes(manifest_data[:target_size])


def generate_file_entry(file_id: int) -> bytes:
    """
    Generate a single file entry for Root Manifest
    
    Simulates V4 DataFile schema:
    - content_type: int32
    - location: string
    - file_format: string
    - partition_date: date32
    - partition_hour: int32
    - record_count: int64
    - file_size_bytes: int64
    - snapshot_id: int64
    - sequence_number: int64
    - status: int32
    """
    entry = bytearray()
    
    # Content type (0 = DATA)
    entry.extend(struct.pack('<I', 0))
    
    # Location (variable length string)
    location = f's3://bucket/table/data/date=2024-01-01/hour=12/file_{file_id:08d}.parquet'
    entry.extend(struct.pack('<H', len(location)))
    entry.extend(location.encode('utf-8'))
    
    # File format
    file_format = 'parquet'
    entry.extend(struct.pack('<H', len(file_format)))
    entry.extend(file_format.encode('utf-8'))
    
    # Partition values
    partition_date = random.randint(0, 1826)  # Days since epoch
    partition_hour = random.randint(0, 23)
    entry.extend(struct.pack('<I', partition_date))
    entry.extend(struct.pack('<I', partition_hour))
    
    # Statistics
    record_count = random.randint(500000, 1500000)
    file_size = random.randint(64 * 1024 * 1024, 192 * 1024 * 1024)
    entry.extend(struct.pack('<Q', record_count))
    entry.extend(struct.pack('<Q', file_size))
    
    # Metadata
    snapshot_id = 1000 + file_id
    sequence_number = file_id
    entry.extend(struct.pack('<Q', snapshot_id))
    entry.extend(struct.pack('<Q', sequence_number))
    
    # Status (0 = EXISTING)
    entry.extend(struct.pack('<I', 0))
    
    return bytes(entry)


def generate_manifest_with_mdvs(
    size_mb: float,
    mdv_inline_ratio: float = 0.1
) -> bytes:
    """
    Generate Root Manifest with inline MDVs
    
    Args:
        size_mb: Target size in megabytes
        mdv_inline_ratio: Ratio of files with inline MDVs (0.0 to 1.0)
        
    Returns:
        Root Manifest with inline MDVs as bytes
    """
    target_size = int(size_mb * 1024 * 1024)
    
    # With MDVs, entries are larger (~2KB each with inline MDV)
    bytes_per_entry_with_mdv = 2048
    bytes_per_entry_without_mdv = 200
    
    # Calculate mix of entries
    avg_entry_size = (
        bytes_per_entry_with_mdv * mdv_inline_ratio +
        bytes_per_entry_without_mdv * (1 - mdv_inline_ratio)
    )
    num_files = int(target_size / avg_entry_size)
    
    manifest_data = bytearray()
    
    # Header
    manifest_data.extend(b'ICEBERG_V4_MANIFEST')
    manifest_data.extend(struct.pack('<I', 1))
    manifest_data.extend(struct.pack('<Q', num_files))
    
    # Generate entries
    for i in range(num_files):
        # Decide if this entry has inline MDV
        has_mdv = random.random() < mdv_inline_ratio
        
        entry = generate_file_entry(i)
        
        if has_mdv:
            # Add inline MDV (simulated)
            mdv_size = random.randint(500, 3500)  # 0.5-3.5 KB
            mdv_data = bytes(random.getrandbits(8) for _ in range(mdv_size))
            entry += struct.pack('<I', mdv_size)
            entry += mdv_data
        
        manifest_data.extend(entry)
        
        if len(manifest_data) >= target_size:
            break
    
    # Pad to exact size
    if len(manifest_data) < target_size:
        padding = target_size - len(manifest_data)
        manifest_data.extend(b'\x00' * padding)
    
    return bytes(manifest_data[:target_size])


def calculate_manifest_size(
    num_files: int,
    avg_entry_size: int = 200,
    mdv_inline_ratio: float = 0.0,
    avg_mdv_size: int = 2048
) -> int:
    """
    Calculate expected Root Manifest size
    
    Args:
        num_files: Number of data files
        avg_entry_size: Average bytes per file entry (without MDV)
        mdv_inline_ratio: Ratio of files with inline MDVs
        avg_mdv_size: Average MDV size in bytes
        
    Returns:
        Expected manifest size in bytes
    """
    # Header size
    header_size = 32
    
    # File entries
    files_without_mdv = int(num_files * (1 - mdv_inline_ratio))
    files_with_mdv = num_files - files_without_mdv
    
    size_without_mdv = files_without_mdv * avg_entry_size
    size_with_mdv = files_with_mdv * (avg_entry_size + avg_mdv_size)
    
    total_size = header_size + size_without_mdv + size_with_mdv
    
    return total_size


def estimate_g1gc_region_size(heap_size_gb: int) -> int:
    """
    Estimate G1GC region size based on heap size
    
    G1GC automatically calculates region size to have ~2048 regions.
    
    Args:
        heap_size_gb: JVM heap size in GB
        
    Returns:
        Region size in MB
    """
    target_regions = 2048
    heap_size_mb = heap_size_gb * 1024
    region_size_mb = heap_size_mb / target_regions
    
    # Round to nearest power of 2 (1, 2, 4, 8, 16, 32 MB)
    powers = [1, 2, 4, 8, 16, 32]
    region_size_mb = min(powers, key=lambda x: abs(x - region_size_mb))
    
    return region_size_mb


def calculate_humongous_threshold(heap_size_gb: int = 4) -> float:
    """
    Calculate Humongous Object threshold for given heap size
    
    Humongous Objects are those > 50% of G1 region size.
    
    Args:
        heap_size_gb: JVM heap size in GB
        
    Returns:
        Humongous threshold in MB
    """
    region_size_mb = estimate_g1gc_region_size(heap_size_gb)
    humongous_threshold_mb = region_size_mb * 0.5
    
    return humongous_threshold_mb


if __name__ == "__main__":
    # Test manifest generation
    print("Testing Root Manifest generation...")
    
    # Generate small manifest
    manifest_1mb = generate_root_manifest(1.0)
    print(f"1 MB manifest: {len(manifest_1mb) / 1024 / 1024:.2f} MB")
    
    # Generate large manifest
    manifest_10mb = generate_root_manifest(10.0)
    print(f"10 MB manifest: {len(manifest_10mb) / 1024 / 1024:.2f} MB")
    
    # Calculate Humongous threshold
    print("\nG1GC Humongous Object thresholds:")
    for heap_gb in [2, 4, 8, 16]:
        region_mb = estimate_g1gc_region_size(heap_gb)
        threshold_mb = calculate_humongous_threshold(heap_gb)
        print(f"  {heap_gb}GB heap: Region={region_mb}MB, Threshold={threshold_mb:.1f}MB")
    
    print("\n✅ Recommendation: Keep Root Manifest < 8MB to avoid Humongous Objects")
