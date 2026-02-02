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
Purpose: Generate sparse MDVs for delete storm testing
"""

import struct
from typing import List
import zlib


def generate_sparse_mdv(total_rows: int, deleted_rows: List[int], file_id: int) -> bytes:
    """
    Generate a sparse MDV (Metadata Delete Vector)
    
    Format (simplified Roaring Bitmap representation):
    - Header: 4 bytes (version)
    - Container count: 4 bytes
    - For each container:
      - Key: 2 bytes (high 16 bits of row ID)
      - Cardinality: 2 bytes
      - Data: variable (bitmap or array)
    
    Args:
        total_rows: Total number of rows in the file
        deleted_rows: List of row IDs that are deleted
        file_id: File identifier
        
    Returns:
        Compressed MDV as bytes
    """
    # Build simple bitmap representation
    mdv_data = bytearray()
    
    # Header
    mdv_data.extend(struct.pack('<I', 1))  # Version 1
    mdv_data.extend(struct.pack('<I', file_id))  # File ID
    mdv_data.extend(struct.pack('<I', total_rows))  # Total rows
    mdv_data.extend(struct.pack('<I', len(deleted_rows)))  # Deleted count
    
    # Deleted row IDs (as array for sparse case)
    for row_id in deleted_rows:
        mdv_data.extend(struct.pack('<I', row_id))
    
    # Compress with zlib (typical for Puffin files)
    compressed = zlib.compress(bytes(mdv_data), level=6)
    
    return compressed


def generate_dense_mdv(total_rows: int, deleted_rows: List[int], file_id: int) -> bytes:
    """
    Generate a dense MDV using bitmap representation
    
    Used when many rows are deleted (>10% of file).
    Results in larger MDV size.
    
    Args:
        total_rows: Total number of rows in the file
        deleted_rows: List of row IDs that are deleted
        file_id: File identifier
        
    Returns:
        Compressed MDV as bytes
    """
    # Build bitmap (1 bit per row)
    bitmap_size = (total_rows + 7) // 8  # Round up to nearest byte
    bitmap = bytearray(bitmap_size)
    
    # Set bits for deleted rows
    for row_id in deleted_rows:
        byte_idx = row_id // 8
        bit_idx = row_id % 8
        bitmap[byte_idx] |= (1 << bit_idx)
    
    # Build MDV
    mdv_data = bytearray()
    mdv_data.extend(struct.pack('<I', 1))  # Version
    mdv_data.extend(struct.pack('<I', file_id))  # File ID
    mdv_data.extend(struct.pack('<I', total_rows))  # Total rows
    mdv_data.extend(struct.pack('<I', len(deleted_rows)))  # Deleted count
    mdv_data.extend(bitmap)  # Bitmap data
    
    # Compress
    compressed = zlib.compress(bytes(mdv_data), level=6)
    
    return compressed


def estimate_mdv_size(total_rows: int, deleted_count: int) -> int:
    """
    Estimate MDV size based on deletion pattern
    
    Args:
        total_rows: Total rows in file
        deleted_count: Number of deleted rows
        
    Returns:
        Estimated size in bytes
    """
    deletion_ratio = deleted_count / total_rows
    
    if deletion_ratio < 0.1:
        # Sparse: Use array representation
        # Header (16 bytes) + 4 bytes per deleted row
        uncompressed = 16 + (deleted_count * 4)
        # Compression ratio ~0.5 for sparse data
        return int(uncompressed * 0.5)
    else:
        # Dense: Use bitmap representation
        # Header (16 bytes) + bitmap
        bitmap_size = (total_rows + 7) // 8
        uncompressed = 16 + bitmap_size
        # Compression ratio ~0.3 for bitmap
        return int(uncompressed * 0.3)


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
    # Test MDV generation
    print("Testing MDV generation...")
    
    # Sparse case: 1 deleted row out of 1000
    sparse_mdv = generate_sparse_mdv(1000, [500], file_id=1)
    print(f"Sparse MDV (1/1000 deleted): {len(sparse_mdv)} bytes")
    
    # Dense case: 500 deleted rows out of 1000
    dense_mdv = generate_dense_mdv(1000, list(range(0, 1000, 2)), file_id=2)
    print(f"Dense MDV (500/1000 deleted): {len(dense_mdv)} bytes")
    
    # Calculate threshold
    threshold = calculate_inline_threshold()
    print(f"\nRecommended inline threshold: {threshold / 1024:.1f} KB")
    print(f"  (Based on 50ms S3 TTFB and 100 MB/s parse rate)")
