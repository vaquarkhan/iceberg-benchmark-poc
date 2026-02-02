#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Quick Test Runner - Generate results for documentation

Runs smaller test cases to generate actual results quickly.
"""

import json
import sys
from pathlib import Path
from datetime import datetime

# Set UTF-8 encoding for Windows console
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

print("="*80)
print("QUICK TEST SUITE - Generating Actual Results")
print("="*80)
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

results_dir = Path("results")
results_dir.mkdir(exist_ok=True)

all_results = {
    'timestamp': datetime.now().isoformat(),
    'tests': {}
}

# Test 1: Delete Storm (smaller scale)
print("\n" + "="*80)
print("TEST 1: DELETE STORM - 4KB Threshold")
print("="*80)

try:
    from test_delete_storm import DeleteStormBenchmark
    
    delete_storm = DeleteStormBenchmark(results_dir)
    result = delete_storm.run_benchmark(num_files=1000)
    
    all_results['tests']['delete_storm'] = {
        'status': 'completed',
        'result': result.to_dict()
    }
    print("✅ Delete Storm Test: PASSED")
except Exception as e:
    print(f"❌ Delete Storm Test: FAILED - {e}")
    all_results['tests']['delete_storm'] = {
        'status': 'failed',
        'error': str(e)
    }

# Test 2: Concurrent Writers (smaller scale)
print("\n" + "="*80)
print("TEST 2: CONCURRENT WRITERS")
print("="*80)

try:
    from test_concurrent_writers import ConcurrentWritersBenchmark
    
    concurrent = ConcurrentWritersBenchmark(results_dir)
    result = concurrent.run_benchmark(num_writers=5, writes_per_writer=50)
    
    all_results['tests']['concurrent_writers'] = {
        'status': 'completed',
        'result': result.to_dict()
    }
    print("✅ Concurrent Writers Test: PASSED")
except Exception as e:
    print(f"❌ Concurrent Writers Test: FAILED - {e}")
    all_results['tests']['concurrent_writers'] = {
        'status': 'failed',
        'error': str(e)
    }

# Test 3: Wide Table (smaller scale)
print("\n" + "="*80)
print("TEST 3: WIDE TABLE PRUNING")
print("="*80)

try:
    from test_wide_table_pruning import WideTableBenchmark
    
    wide_table = WideTableBenchmark(results_dir)
    result = wide_table.run_benchmark(num_columns=500, num_files=50)
    
    all_results['tests']['wide_table'] = {
        'status': 'completed',
        'result': result.to_dict()
    }
    print("✅ Wide Table Test: PASSED")
except Exception as e:
    print(f"❌ Wide Table Test: FAILED - {e}")
    all_results['tests']['wide_table'] = {
        'status': 'failed',
        'error': str(e)
    }

# Test 4: Adaptive Tree
print("\n" + "="*80)
print("TEST 4: ADAPTIVE TREE")
print("="*80)

try:
    from utils.adaptive_tree import AdaptiveTreeManager, DataFileEntry
    from pathlib import Path
    
    manager = AdaptiveTreeManager(output_dir=Path("test_metadata"))
    
    # Simulate commits
    for i in range(100):
        entry = DataFileEntry(
            file_id=i,
            file_path=f"s3://bucket/data/file_{i}.parquet",
            record_count=1000000,
            file_size_bytes=128 * 1024 * 1024,
            partition_values={'date': '2024-01-01'},
            mdv=b'\x00' * 2048
        )
        manager.commit_file(entry)
    
    stats = manager.get_tree_stats()
    
    all_results['tests']['adaptive_tree'] = {
        'status': 'completed',
        'result': stats
    }
    print("✅ Adaptive Tree Test: PASSED")
    print(f"   Tree stats: {stats}")
except Exception as e:
    print(f"❌ Adaptive Tree Test: FAILED - {e}")
    all_results['tests']['adaptive_tree'] = {
        'status': 'failed',
        'error': str(e)
    }

# Save results
output_file = results_dir / "quick_test_results.json"
with open(output_file, 'w') as f:
    json.dump(all_results, f, indent=2)

# Print summary
print("\n" + "="*80)
print("QUICK TEST SUITE COMPLETE")
print("="*80)
print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Results saved to: {output_file}")

passed = sum(1 for t in all_results['tests'].values() if t['status'] == 'completed')
failed = sum(1 for t in all_results['tests'].values() if t['status'] == 'failed')

print(f"\nTests Passed: {passed}/{len(all_results['tests'])}")
print(f"Tests Failed: {failed}/{len(all_results['tests'])}")

if failed == 0:
    print("\n🎉 ALL TESTS PASSED!")
    print("\n📊 Key Results:")
    
    if 'delete_storm' in all_results['tests'] and all_results['tests']['delete_storm']['status'] == 'completed':
        ds = all_results['tests']['delete_storm']['result']
        print(f"   • Delete Storm: {ds['speedup_factor']:.1f}x speedup with inline MDVs")
    
    if 'concurrent_writers' in all_results['tests'] and all_results['tests']['concurrent_writers']['status'] == 'completed':
        cw = all_results['tests']['concurrent_writers']['result']
        print(f"   • Concurrent Writers: {cw['conflict_rate']*100:.1f}% conflict rate, {cw['throughput_commits_per_sec']:.0f} commits/sec")
    
    if 'wide_table' in all_results['tests'] and all_results['tests']['wide_table']['status'] == 'completed':
        wt = all_results['tests']['wide_table']['result']
        print(f"   • Wide Table: {wt['speedup_factor']:.1f}x speedup with V4 stats")
    
    if 'adaptive_tree' in all_results['tests'] and all_results['tests']['adaptive_tree']['status'] == 'completed':
        at = all_results['tests']['adaptive_tree']['result']
        print(f"   • Adaptive Tree: {at['tree_depth']} depth, {at['total_flushes']} flushes")

print("="*80)
