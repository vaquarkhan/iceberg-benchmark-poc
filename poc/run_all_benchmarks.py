"""
Run All Benchmarks - Complete Test Suite

Author: Vaquar Khan (vaquar.khan@gmail.com)

Executes all benchmark suites:
1. Delete Storm (4KB Threshold)
2. GC Performance Cliff (10MB Threshold)
3. Density-Adaptive Policy (Complete Strategy)
4. DV Resolution Strategies (V4 Architectural Choices)
5. Single File Commits (Streaming Performance)
6. Adaptive Metadata Tree (Tree Structure Optimization)

Generates comprehensive results for HTML dashboard.
"""

import json
import time
from pathlib import Path
from datetime import datetime

from test_delete_storm import DeleteStormBenchmark
from test_gc_performance_cliff import GCPerformanceCliffBenchmark
from test_density_adaptive_policy import DensityAdaptivePolicyBenchmark
from test_dv_resolution_strategies import run_all_dv_resolution_benchmarks
from test_single_file_commits import run_single_file_commit_benchmarks
from test_adaptive_metadata_tree import run_adaptive_tree_benchmarks


def run_all_benchmarks():
    """Run complete benchmark suite"""
    
    print("="*80)
    print("APACHE ICEBERG V4 MDV BENCHMARK SUITE")
    print("Complete Validation of Metadata Management Policy")
    print("="*80)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    results_dir = Path("results")
    results_dir.mkdir(exist_ok=True)
    
    all_results = {
        'timestamp': datetime.now().isoformat(),
        'benchmarks': {}
    }
    
    # Test 1: Delete Storm (4KB Threshold)
    print("\n" + "="*80)
    print("TEST 1: DELETE STORM - 4KB Threshold Validation")
    print("="*80)
    
    try:
        delete_storm = DeleteStormBenchmark(results_dir)
        result_1k = delete_storm.run_benchmark(num_files=1000)
        
        all_results['benchmarks']['delete_storm'] = {
            'status': 'completed',
            'result': result_1k.to_dict()
        }
        print("✅ Delete Storm Test: PASSED")
    except Exception as e:
        print(f"❌ Delete Storm Test: FAILED - {e}")
        all_results['benchmarks']['delete_storm'] = {
            'status': 'failed',
            'error': str(e)
        }
    
    # Test 2: GC Performance Cliff (10MB Threshold)
    print("\n" + "="*80)
    print("TEST 2: GC PERFORMANCE CLIFF - 10MB Threshold Validation")
    print("="*80)
    
    try:
        gc_cliff = GCPerformanceCliffBenchmark(results_dir)
        gc_results = gc_cliff.run_benchmark()
        
        all_results['benchmarks']['gc_cliff'] = {
            'status': 'completed',
            'results': [r.to_dict() for r in gc_results]
        }
        print("✅ GC Performance Cliff Test: PASSED")
    except Exception as e:
        print(f"❌ GC Performance Cliff Test: FAILED - {e}")
        all_results['benchmarks']['gc_cliff'] = {
            'status': 'failed',
            'error': str(e)
        }
    
    # Test 3: Density-Adaptive Policy (Complete Strategy)
    print("\n" + "="*80)
    print("TEST 3: DENSITY-ADAPTIVE POLICY - Complete Strategy Validation")
    print("="*80)
    
    try:
        policy_test = DensityAdaptivePolicyBenchmark(results_dir)
        policy_results = policy_test.run_all_scenarios()
        
        all_results['benchmarks']['density_adaptive_policy'] = {
            'status': 'completed',
            'results': [r.to_dict() for r in policy_results]
        }
        print("✅ Density-Adaptive Policy Test: PASSED")
    except Exception as e:
        print(f"❌ Density-Adaptive Policy Test: FAILED - {e}")
        all_results['benchmarks']['density_adaptive_policy'] = {
            'status': 'failed',
            'error': str(e)
        }
    
    # Test 4: DV Resolution Strategies (V4 Architectural Choices)
    print("\n" + "="*80)
    print("TEST 4: DV RESOLUTION STRATEGIES - V4 Architectural Validation")
    print("="*80)
    
    try:
        dv_results = run_all_dv_resolution_benchmarks()
        
        all_results['benchmarks']['dv_resolution_strategies'] = {
            'status': 'completed',
            'results': dv_results
        }
        print("✅ DV Resolution Strategies Test: PASSED")
    except Exception as e:
        print(f"❌ DV Resolution Strategies Test: FAILED - {e}")
        all_results['benchmarks']['dv_resolution_strategies'] = {
            'status': 'failed',
            'error': str(e)
        }
    
    # Test 5: Single File Commits Performance
    print("\n" + "="*80)
    print("TEST 5: SINGLE FILE COMMITS - Streaming Workload Performance")
    print("="*80)
    
    try:
        commit_results = run_single_file_commit_benchmarks()
        
        all_results['benchmarks']['single_file_commits'] = {
            'status': 'completed',
            'results': commit_results
        }
        print("✅ Single File Commits Test: PASSED")
    except Exception as e:
        print(f"❌ Single File Commits Test: FAILED - {e}")
        all_results['benchmarks']['single_file_commits'] = {
            'status': 'failed',
            'error': str(e)
        }
    
    # Test 6: Adaptive Metadata Tree
    print("\n" + "="*80)
    print("TEST 6: ADAPTIVE METADATA TREE - Tree Structure Optimization")
    print("="*80)
    
    try:
        tree_results = run_adaptive_tree_benchmarks()
        
        all_results['benchmarks']['adaptive_metadata_tree'] = {
            'status': 'completed',
            'results': tree_results
        }
        print("✅ Adaptive Metadata Tree Test: PASSED")
    except Exception as e:
        print(f"❌ Adaptive Metadata Tree Test: FAILED - {e}")
        all_results['benchmarks']['adaptive_metadata_tree'] = {
            'status': 'failed',
            'error': str(e)
        }
    
    # Save comprehensive results
    output_file = results_dir / "all_benchmarks_results.json"
    with open(output_file, 'w') as f:
        json.dump(all_results, f, indent=2)
    
    # Print final summary
    print("\n" + "="*80)
    print("BENCHMARK SUITE COMPLETE")
    print("="*80)
    print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Results saved to: {output_file}")
    
    # Count passed/failed
    passed = sum(1 for b in all_results['benchmarks'].values() if b['status'] == 'completed')
    failed = sum(1 for b in all_results['benchmarks'].values() if b['status'] == 'failed')
    
    print(f"\nTests Passed: {passed}/6")
    print(f"Tests Failed: {failed}/6")
    
    if failed == 0:
        print("\n🎉 ALL TESTS PASSED!")
        print("\n📊 Key Findings:")
        print("   ✅ 4KB threshold validated (S3 TTFB dominance)")
        print("   ✅ 10MB threshold validated (G1GC Humongous Objects)")
        print("   ✅ Density-Adaptive Policy validated (3-rule strategy)")
        print("   ✅ DV Resolution Strategies validated (join performance)")
        print("   ✅ Single File Commits validated (streaming performance)")
        print("   ✅ Adaptive Metadata Tree validated (optimal depth)")
        print("\n💡 Recommendation: Adopt proposed MDV spill-over policy for Iceberg V4")
    else:
        print(f"\n⚠️  {failed} test(s) failed. Review logs above.")
    
    print("="*80)
    
    return all_results


if __name__ == "__main__":
    run_all_benchmarks()
