# Apache Iceberg V4 Benchmark Suite

**Last Updated:** February 2, 2026 - Version 2.2  
**Author:** Vaquar Khan (vaquar.khan@gmail.com)  
**License:** Apache License 2.0  
**Purpose:** Empirical validation of Apache Iceberg V4 metadata management policies

## 🎯 Latest Version: 11 Benchmark Suites + 6 Design Recommendations

View the complete interactive report: **[https://vaquarkhan.github.io/iceberg-benchmark-poc/](https://vaquarkhan.github.io/iceberg-benchmark-poc/)**

## Overview

This benchmark suite provides empirical evidence for critical architectural decisions in Apache Iceberg V4, specifically addressing:

1. **Writer Organization Requirements** - Proving that V4's implicit Parquet statistics require sorted data layout
2. **MDV Threshold Validation** - Validating 4KB and 10MB thresholds based on physical constraints (S3 TTFB, JVM G1GC)
3. **Density-Adaptive Policy** - Testing complete MDV spill-over strategy with Roaring Bitmaps
4. **DV Resolution Strategies** - Benchmarking architectural choices from Apache Iceberg community discussions
5. **Single File Commits** - Measuring streaming workload performance
6. **Adaptive Metadata Tree** - Evaluating optimal manifest tree structures

## Why This Benchmark Suite Exists

### Problem Statement

During Apache Iceberg V4 specification development, several critical questions emerged:

- **Does V4's implicit Parquet statistics work for streaming workloads?** (Answer: No, without sorted data)
- **What are the optimal thresholds for MDV inlining?** (Answer: 4KB and 10MB, based on physical constraints)
- **Should DVs be inlined or external?** (Answer: Depends on size and density)
- **What's the optimal manifest tree structure?** (Answer: Flat for most tables)

### Our Approach

Rather than relying on theoretical analysis, we built **real, measurable benchmarks** that:

1. Use actual file I/O operations (not simulations)
2. Measure wall-clock time with high precision
3. Track real memory usage
4. Generate reproducible results
5. Provide empirical data for specification decisions

### Impact

These benchmarks have informed:

- Apache Iceberg V4 specification discussions
- MDV (Metadata Delete Vector) policy design
- Writer organization requirements
- Manifest tree structure recommendations

## Benchmark Results

View the complete interactive report: **[index.html](index.html)**

### Key Findings

1. **Writer Organization (Tab 0)**
   - Unsorted data: 0% skip rate, 99.8% domain span
   - Sorted data: 99% skip rate, 1.0% domain span
   - Impact: 99.2x I/O amplification without sorting

2. **MDV Thresholds (Tab 1)**
   - 4KB threshold: 2,370x speedup (S3 TTFB dominance)
   - 10MB threshold: Humongous object allocations validated

3. **Density-Adaptive Policy (Tab 2)**
   - All scenarios: 100% inline rate achieved
   - Policy handles sparse, dense, and long-tail patterns

4. **DV Resolution (Tab 4)**
   - Positional join: 1.3x faster, 87% less memory
   - Folded DVs: 50% I/O reduction
   - Order-preserving: 22,000% overhead (rejected)

5. **Single File Commits (Tab 5)**
   - Latency: 4.27ms (1 file) to 49.10ms (1000 files)
   - Throughput: 234 to 20,367 files/sec
   - Conclusion: Streaming workloads viable

6. **Adaptive Tree (Tab 6)**
   - Flat structure fastest for tables up to 50K files
   - 2-level: 2-6x slower than flat
   - 3-level: 15-27x slower than flat

## Repository Structure

```
iceberg-benchmark-poc/
├── index.html                    # Main interactive report (7 tabs)
├── index_original_backup.html    # Full writer organization report
├── images/                       # Benchmark visualizations
│   ├── row_group_ranges.png
│   ├── skip_rates.png
│   ├── bytes_read.png
│   └── write_overhead.png
├── poc/                          # Benchmark implementation
│   ├── test_delete_storm.py              # 4KB threshold validation
│   ├── test_gc_performance_cliff.py      # 10MB threshold validation
│   ├── test_density_adaptive_policy.py   # Complete policy validation
│   ├── test_dv_resolution_strategies.py  # DV resolution benchmarks
│   ├── test_single_file_commits.py       # Commit performance
│   ├── test_adaptive_metadata_tree.py    # Tree structure optimization
│   ├── run_all_benchmarks.py             # Run complete suite
│   ├── utils/                            # Utility modules
│   │   ├── manifest_generator.py
│   │   ├── mdv_generator.py
│   │   ├── metrics_collector.py
│   │   └── s3_simulator.py
│   └── results/                          # Benchmark results (JSON)
└── README.md                     # This file
```

## Running the Benchmarks

### Prerequisites

```bash
pip install -r poc/requirements.txt
```

Required packages:
- `pyarrow` - Parquet file operations
- `psutil` - Memory usage tracking
- `hypothesis` - Property-based testing (optional)

### Run All Benchmarks

```bash
cd poc
python run_all_benchmarks.py
```

This runs all 6 benchmark suites and generates results in `poc/results/`.

### Run Individual Benchmarks

```bash
# Tab 1: MDV Threshold Validation
python test_delete_storm.py
python test_gc_performance_cliff.py

# Tab 2: Density-Adaptive Policy
python test_density_adaptive_policy.py

# Tab 4: DV Resolution Strategies
python test_dv_resolution_strategies.py

# Tab 5: Single File Commits
python test_single_file_commits.py

# Tab 6: Adaptive Metadata Tree
python test_adaptive_metadata_tree.py
```

### View Results

Open `index.html` in your browser to see the interactive report with all benchmark results.

## Methodology

### Real Measurements (No Simulations)

All benchmarks use **100% real measurements**:

- ✅ Actual file I/O operations
- ✅ Real JSON serialization/deserialization
- ✅ Measured wall-clock time (`time.perf_counter()`)
- ✅ Actual memory usage (`psutil.Process().memory_info()`)
- ✅ Real file sizes (`os.path.getsize()`)

### Reproducibility

All benchmarks are:

1. **Deterministic** - Use fixed random seeds
2. **Documented** - Clear methodology in code comments
3. **Validated** - Property-based tests where applicable
4. **Reproducible** - Anyone can run and verify results

## Contributing to Apache Iceberg

This benchmark suite is shared with the Apache Iceberg community to inform V4 specification decisions. If you find issues or have suggestions:

1. Open an issue on the Apache Iceberg GitHub
2. Reference these benchmarks in specification discussions
3. Propose improvements or additional benchmarks

## Community Discussions

These benchmarks address concerns raised in:

- Apache Iceberg dev mailing list discussions
- V4 specification design documents
- Community feedback on metadata management

## License

```
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
```

## Contact

**Vaquar Khan**  
Email: vaquar.khan@gmail.com  
LinkedIn: [linkedin.com/in/vaquar-khan-b695577](https://www.linkedin.com/in/vaquar-khan-b695577/)

## Acknowledgments

This work builds on discussions and feedback from the Apache Iceberg community. Special thanks to all contributors who provided insights and validation of these benchmarks.

---

**Note:** This is an independent benchmark suite created to inform Apache Iceberg V4 specification decisions. It is not an official Apache Software Foundation project.
