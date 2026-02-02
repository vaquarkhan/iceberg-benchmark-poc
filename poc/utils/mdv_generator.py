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
MDV (Metadata Delete Vector) Generator
================================================================================
Author: Vaquar Khan (vaquar.khan@gmail.com)
Purpose: Generate MDVs using actual Roaring Bitmap implementation
"""

import struct
from typing import List
import zlib

try:
    from pyroaring import BitMap
    ROARING_AVAILABLE = True
except ImportError:
    ROARING_AVAILABLE = False
    print("WARNING: pyroaring not installed. Install with: pip install pyroaring>=0.4.0")


def generate_sparse_mdv(total_rows: int, deleted_rows: List[int], file_id: int) -> bytes:
    """
    Generate a sparse MDV using ACTUAL Roaring Bitmap
    
    Uses pyroaring library for production-grade bitmap compression.
    Automatically selects optimal container type (Array/Bitmap/Run).
    
    Args:
        total_rows: Total number of rows in the file
        deleted_rows: List of row IDs that are deleted
        file_id: File identifier
        
    Returns:
        Compressed MDV as bytes
    """
    if not ROARING_AVAILABLE:
        raise RuntimeError("pyroaring is required. Install with: pip install pyroaring>=0.4.0")
    
    # Create Roaring Bitmap with deleted row IDs
    bitmap = BitMap(deleted_rows)
    
    # Serialize bitmap (uses optimal container representation)
    bitmap_bytes = bitmap.serialize()
    
    # Build MDV with header
    mdv_data = bytearray()
    mdv_data.extend(struct.pack('<I', 1))  # Version 1
    mdv_data.extend(struct.pack('<I', file_id))  # File ID
    mdv_data.extend(struct.pack('<I', total_rows))  # Total rows
    mdv_data.extend(struct.pack('<I', len(deleted_rows)))  # Deleted count
    mdv_data.extend(bitmap_bytes)  # Roaring Bitmap serialized data
    
    # Compress with zlib (Puffin file format)
    compressed = zlib.compress(bytes(mdv_data), level=6)
    
    return compressed


def generate_dense_mdv(total_rows: int, deleted_rows: List[int], file_id: int) -> bytes:
    """
    Generate a dense MDV using ACTUAL Roaring Bitmap
    
    Roaring Bitmap automatically uses Bitmap containers for dense deletions.
    
    Args:
        total_rows: Total number of rows in the file
        deleted_rows: List of row IDs that are deleted
        file_id: File identifier
        
    Returns:
        Compressed MDV as bytes
    """
    if not ROARING_AVAILABLE:
        raise RuntimeError("pyroaring is required. Install with: pip install pyroaring>=0.4.0")
    
    # Create Roaring Bitmap - it will automatically use Bitmap container for dense data
    bitmap = BitMap(deleted_rows)
    
    # Serialize
    bitmap_bytes = bitmap.serialize()
    
    # Build MDV
    mdv_data = bytearray()
    mdv_data.extend(struct.pack('<I', 1))  # Version
    mdv_data.extend(struct.pack('<I', file_id))  # File ID
    mdv_data.extend(struct.pack('<I', total_rows))  # Total rows
    mdv_data.extend(struct.pack('<I', len(deleted_rows)))  # Deleted count
    mdv_data.extend(bitmap_bytes)
    
    # Compress
    compressed = zlib.compress(bytes(mdv_data), level=6)
    
    return compressed


def generate_run_encoded_mdv(total_rows: int, deleted_rows: List[int], file_id: int) -> bytes:
    """
    Generate MDV with Run-Length Encoding for contiguous deletions
    
    Roaring Bitmap automatically uses Run containers for contiguous ranges.
    This is optimal for partition drops or time-range deletions.
    
    Args:
        total_rows: Total number of rows in the file
        deleted_rows: List of row IDs (should be contiguous for best compression)
        file_id: File identifier
        
    Returns:
        Compressed MDV as bytes
    """
    if not ROARING_AVAILABLE:
        raise RuntimeError("pyroaring is required. Install with: pip install pyroaring>=0.4.0")
    
    # Create Roaring Bitmap - will use Run container for contiguous ranges
    bitmap = BitMap(deleted_rows)
    
    # Optimize to ensure Run containers are used where possible
    bitmap.run_optimize()
    
    # Serialize
    bitmap_bytes = bitmap.serialize()
    
    # Build MDV
    mdv_data = bytearray()
    mdv_data.extend(struct.pack('<I', 1))  # Version
    mdv_data.extend(struct.pack('<I', file_id))  # File ID
    mdv_data.extend(struct.pack('<I', total_rows))  # Total rows
    mdv_data.extend(struct.pack('<I', len(deleted_rows)))  # Deleted count
    mdv_data.extend(bitmap_bytes)
    
    # Compress
    compressed = zlib.compress(bytes(mdv_data), level=6)
    
    return compressed


def get_container_stats(deleted_rows: List[int]) -> dict:
    """
    Analyze which Roaring Bitmap container types are used
    
    Returns:
        Dictionary with container type statistics
    """
    if not ROARING_AVAILABLE:
        return {'error': 'pyroaring not available'}
    
    bitmap = BitMap(deleted_rows)
    bitmap.run_optimize()
    
    serialized = bitmap.serialize()
    
    return {
        'cardinality': len(bitmap),
        'size_bytes': len(serialized),
        'size_compressed': len(zlib.compress(serialized)),
        'is_run_optimized': True  # After run_optimize() call
    }


def estimate_mdv_size(total_rows: int, deleted_count: int) -> int:
    """
    Estimate MDV size using actual Roaring Bitmap compression
    
    Args:
        total_rows: Total rows in file
        deleted_count: Number of deleted rows
        
    Returns:
        Estimated size in bytes
    """
    if not ROARING_AVAILABLE:
        # Fallback estimation
        deletion_ratio = deleted_count / total_rows
        if deletion_ratio < 0.1:
            uncompressed = 16 + (deleted_count * 4)
            return int(uncompressed * 0.5)
        else:
            bitmap_size = (total_rows + 7) // 8
            uncompressed = 16 + bitmap_size
            return int(uncompressed * 0.3)
    
    # Use actual Roaring Bitmap for accurate estimation
    sample_rows = list(range(0, deleted_count))
    bitmap = BitMap(sample_rows)
    bitmap.run_optimize()
    
    serialized = bitmap.serialize()
    compressed = zlib.compress(serialized, level=6)
    
    # Add header overhead
    return 16 + len(compressed)


def calculate_inline_threshold(avg_s3_ttfb_ms: float = 50.0) -> int:
    """
    Calculate optimal MDV inline threshold based on S3 TTFB
    
    The threshold should be set where:
    - Cost of inlining (larger manifest) < Cost of external fetch (TTFB)
    
    Args:
        avg_s3_ttfb_ms: Average S3 Time To First Byte in milliseconds
        
    Returns:
        Recommended threshold in bytes
    """
    # Assumptions:
    # - Manifest parse rate: 100 MB/s
    # - S3 TTFB: 50ms (typical)
    # - Break-even: Parse time = TTFB
    
    parse_rate_bytes_per_ms = (100 * 1024 * 1024) / 1000  # 100 MB/s
    
    # Bytes that can be parsed in TTFB time
    threshold = int(parse_rate_bytes_per_ms * avg_s3_ttfb_ms)
    
    # Round to nearest KB
    threshold_kb = (threshold + 512) // 1024
    
    return threshold_kb * 1024


if __name__ == "__main__":
    # Test MDV generation with ACTUAL Roaring Bitmaps
    print("Testing MDV generation with pyroaring...")
    
    if not ROARING_AVAILABLE:
        print("ERROR: pyroaring not installed!")
        print("Install with: pip install pyroaring>=0.4.0")
        exit(1)
    
    # Sparse case: 1 deleted row out of 1000
    print("\n1. Sparse MDV (1/1000 deleted):")
    sparse_mdv = generate_sparse_mdv(1000, [500], file_id=1)
    print(f"   Size: {len(sparse_mdv)} bytes")
    stats = get_container_stats([500])
    print(f"   Container stats: {stats}")
    
    # Dense case: 500 deleted rows out of 1000
    print("\n2. Dense MDV (500/1000 deleted):")
    dense_mdv = generate_dense_mdv(1000, list(range(0, 1000, 2)), file_id=2)
    print(f"   Size: {len(dense_mdv)} bytes")
    stats = get_container_stats(list(range(0, 1000, 2)))
    print(f"   Container stats: {stats}")
    
    # Run-encoded case: Contiguous range deletion
    print("\n3. Run-Encoded MDV (contiguous 100-599 deleted):")
    run_mdv = generate_run_encoded_mdv(1000, list(range(100, 600)), file_id=3)
    print(f"   Size: {len(run_mdv)} bytes")
    stats = get_container_stats(list(range(100, 600)))
    print(f"   Container stats: {stats}")
    
    # Calculate threshold
    threshold = calculate_inline_threshold()
    print(f"\n4. Recommended inline threshold: {threshold / 1024:.1f} KB")
    print(f"   (Based on 50ms S3 TTFB and 100 MB/s parse rate)")
    
    print("\n✅ All tests use ACTUAL Roaring Bitmap implementation (pyroaring)")
