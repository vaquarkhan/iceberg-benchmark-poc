# Column Families Benchmark

## Overview

This benchmark validates **Peter Vary's Column Families proposal** for Apache Iceberg using real AWS S3 I/O and JVM operations.

**Reference:**
- GitHub PR: https://github.com/apache/iceberg/pull/13306
- Email: Peter Vary to dev@iceberg.apache.org (February 2026)
- Google Doc: https://docs.google.com/document/d/1OHuZ6RyzZvCOQ6UQoV84GzwVp3UPiu_cfXClsOi03ww

## What is Column Families?

Column Families is a proposed feature for Apache Iceberg that enables:

1. **Vertical Data Partitioning** - Split columns across multiple Parquet files per row group
2. **Partial Column Updates** - Rewrite only changed columns, not entire rows
3. **Efficient Wide Tables** - Handle 1000+ column tables without metadata bloat
4. **ML Workload Optimization** - Feature backfills without full table rewrites

### Architecture

**Current (Single File):**
```
Row Group 1: [col1, col2, col3, ..., col1000] → file1.parquet
Row Group 2: [col1, col2, col3, ..., col1000] → file2.parquet
```

**Column Families (Vertical Split):**
```
Row Group 1: 
  - Family 1: [col1-col200]   → file1_fam1.parquet
  - Family 2: [col201-col400] → file1_fam2.parquet
  - Family 3: [col401-col600] → file1_fam3.parquet
  - Family 4: [col601-col800] → file1_fam4.parquet
  - Family 5: [col801-col1000] → file1_fam5.parquet
```

## Peter Vary's Results (Local SSD)

From his preliminary benchmarks on Apple M4 Pro with local SSDs:

### Read Performance (100 columns)
- **Baseline (0 families):** 3.739s
- **2 families (single-threaded):** 4.036s (~8% slowdown)
- **2 families (multi-threaded):** 4.063s (~9% slowdown)

### Write Performance (100 columns)
- **Baseline (0 families):** 32.397s
- **2 families (parallel):** 19.974s (~38% speedup!)

### Key Findings
- Read overhead: ~10% with column stitching
- Write speedup: ~38% with parallel writes
- Multi-threading: Limited benefit on local SSD (I/O bottleneck)

## Our Benchmark: Real S3 + JVM

This benchmark extends Peter's work by testing with **real AWS S3** to answer:

1. **Does blob storage latency change the performance characteristics?**
2. **Can multi-threaded fetching absorb the ~10% read overhead?**
3. **Are column families viable for production ML workloads?**

## Test Scenarios

### Test 1: Read Performance (100 columns)
- Baseline: Single file with all columns
- 2 families: Split 50-50
- Measure: Single-threaded vs multi-threaded reads
- **Expected:** Multi-threading should absorb overhead with S3 latency

### Test 2: Read Performance (1000 columns)
- Baseline: Single file with 1000 columns
- 5 families: Split 200 columns each
- Measure: Single-threaded vs multi-threaded reads
- **Expected:** Benefits more apparent with wider tables

### Test 3: Write Performance (100 columns)
- Baseline: Write all columns to single file
- 2 families: Parallel writes to 2 files
- **Expected:** Parallel writes provide speedup

### Test 4: Write Performance (1000 columns)
- Baseline: Write all columns to single file
- 5 families: Parallel writes to 5 files
- **Expected:** Greater speedup with more families

### Test 5: Partial Column Update
- Scenario: Update 1 column in 1000-column table
- Baseline: Full table rewrite (all 1000 columns)
- Column families: Rewrite only 1 family (200 columns)
- **Expected:** 80% write amplification reduction

### Test 6: Narrow Read (ML Workload)
- Scenario: Read 5 columns from 1000-column table
- Baseline: Read entire file, project 5 columns
- Column families: Read only 1 family (200 columns)
- **Expected:** Significant I/O reduction

## Running the Benchmark

### Prerequisites

1. **AWS Account** with S3 access
2. **Java 11+** and **Maven 3.6+**
3. **AWS CLI** configured

### Setup

```bash
# 1. Configure AWS credentials
aws configure

# 2. Create S3 bucket
aws s3 mb s3://iceberg-column-families-test

# 3. Build project
cd real-benchmarks/java
mvn clean install

# 4. Run benchmark
java -jar target/iceberg-v4-benchmarks.jar \
  --benchmark ColumnFamilies \
  --s3-bucket iceberg-column-families-test \
  --num-files 100
```

### Command Line Options

```
--benchmark ColumnFamilies    (required) Benchmark type
--s3-bucket <name>            (required) S3 bucket for test data
--num-files <n>               (optional) Number of files per test (default: 100)
--aws-region <region>         (optional) AWS region (default: us-east-1)
--output-dir <path>           (optional) Results directory (default: ./results)
```

## Expected Results

Based on Peter's findings and S3 characteristics:

### Read Performance
- **Local SSD:** ~10% overhead (Peter's result)
- **S3 (our test):** <15% overhead with multi-threading
- **Reason:** S3 latency allows parallel fetches to overlap

### Write Performance
- **Local SSD:** ~38% speedup (Peter's result)
- **S3 (our test):** 30-40% speedup
- **Reason:** Parallel writes benefit from multi-core compression

### Partial Updates
- **Write amplification reduction:** 80%+
- **Use case:** ML feature backfills, column additions

### Narrow Reads
- **I/O reduction:** 50%+ for reading 5 columns from 1000
- **Use case:** ML inference, feature selection

## Interpreting Results

### ✅ Column Families are VIABLE if:
- Read overhead < 15% with multi-threading
- Write speedup > 30% with parallel writes
- Partial updates reduce write amplification > 80%
- Narrow reads show > 50% I/O reduction

### ⚠️ Use with CAUTION if:
- Read overhead > 20% even with multi-threading
- Full table scans are common (not ML workloads)
- Tables have < 100 columns (overhead not worth it)

## Cost Estimate

**AWS S3 Costs:**
- Storage: ~$0.023/GB/month
- PUT requests: $0.005 per 1,000
- GET requests: $0.0004 per 1,000

**Estimated cost for full benchmark:** ~$2-5

## Cleanup

```bash
# Delete all test data
aws s3 rm s3://iceberg-column-families-test --recursive

# Delete bucket
aws s3 rb s3://iceberg-column-families-test
```

## Results Format

Results are saved as JSON in `results/column_families_results_<timestamp>.json`:

```json
{
  "benchmarkName": "ColumnFamilies",
  "totalDurationMs": 45000,
  "filesProcessed": 100,
  "s3Operations": 1000,
  "metrics": {
    "read_100_cols_baseline_ms": 3739,
    "read_100_cols_2_families_ms": 4036,
    "read_100_cols_2_families_parallel_ms": 4063,
    "write_100_cols_baseline_ms": 32397,
    "write_100_cols_2_families_parallel_ms": 19974,
    ...
  }
}
```

## Comparison with Peter's Results

| Metric | Peter (Local SSD) | Our Benchmark (S3) |
|--------|-------------------|-------------------|
| Read overhead (100 cols) | 8% | TBD |
| Read overhead (parallel) | 9% | TBD |
| Write speedup (100 cols) | 38% | TBD |
| Multi-threading benefit | Minimal | TBD |

## Contributing to Apache Iceberg

These results will be shared with the Apache Iceberg community to inform the Column Families proposal decision.

**Discussion threads:**
- dev@iceberg.apache.org
- GitHub PR #13306

## References

1. Peter Vary's PR: https://github.com/apache/iceberg/pull/13306
2. Column Families Design Doc: https://docs.google.com/document/d/1OHuZ6RyzZvCOQ6UQoV84GzwVp3UPiu_cfXClsOi03ww
3. Apache Iceberg V4 Spec: https://iceberg.apache.org/

## License

Apache License 2.0 (same as Apache Iceberg)

## Author

**Vaquar Khan**  
Email: vaquar.khan@gmail.com  
Based on Peter Vary's preliminary benchmarks
