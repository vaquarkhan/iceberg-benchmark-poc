# Real Benchmarks - Java + AWS S3

This directory contains **production-grade benchmarks** using:
- ✅ Real AWS S3 (not simulated)
- ✅ Java/JVM (not Python)
- ✅ Actual Apache Iceberg operations
- ✅ Real network I/O

## Purpose

Address feedback from Apache Iceberg committers that Python simulations don't reflect real-world performance.

## Structure

```
real-benchmarks/
├── java/                          # Java-based benchmarks
│   ├── pom.xml                   # Maven dependencies
│   ├── src/main/java/
│   │   └── org/apache/iceberg/benchmarks/
│   │       ├── DeleteStormBenchmark.java
│   │       ├── DVResolutionBenchmark.java
│   │       └── WriteOverheadBenchmark.java
│   └── src/test/java/
├── terraform/                     # AWS infrastructure
│   ├── main.tf                   # S3 bucket, IAM roles
│   └── variables.tf
├── scripts/                       # Helper scripts
│   ├── setup-aws.sh
│   └── run-benchmarks.sh
└── results/                       # Real benchmark results
    └── .gitkeep
```

## Prerequisites

1. **Java 11+**
   ```bash
   java -version
   ```

2. **Maven 3.6+**
   ```bash
   mvn -version
   ```

3. **AWS Account**
   - AWS CLI configured
   - S3 bucket access
   - IAM permissions

4. **Apache Iceberg**
   - Version 1.5.0+
   - With AWS integration

## Setup

### 1. Configure AWS Credentials

```bash
aws configure
# Enter your AWS Access Key ID
# Enter your AWS Secret Access Key
# Default region: us-east-1
```

### 2. Create S3 Bucket

```bash
aws s3 mb s3://iceberg-v4-benchmarks
```

### 3. Build Java Project

```bash
cd java
mvn clean install
```

### 4. Run Benchmarks

```bash
./scripts/run-benchmarks.sh
```

## Benchmarks

### 1. Delete Storm (Inline vs External DVs)

**Measures:** Query planning time with real S3 I/O

```bash
java -jar target/iceberg-benchmarks.jar \
  --benchmark DeleteStorm \
  --s3-bucket iceberg-v4-benchmarks \
  --num-files 10000
```

**What it does:**
- Creates real Parquet files on S3
- Generates real delete vectors
- Measures actual query planning time
- Uses real Iceberg catalog operations

### 2. DV Resolution (Hash vs Positional Joins)

**Measures:** Memory usage and join performance

```bash
java -jar target/iceberg-benchmarks.jar \
  --benchmark DVResolution \
  --s3-bucket iceberg-v4-benchmarks \
  --num-files 25000
```

**What it does:**
- Real manifest file operations
- Actual hash table vs positional array
- JVM memory profiling
- Real file path resolution

### 3. Write Overhead (Sorted vs Unsorted)

**Measures:** Commit time with real file writes

```bash
java -jar target/iceberg-benchmarks.jar \
  --benchmark WriteOverhead \
  --s3-bucket iceberg-v4-benchmarks \
  --num-commits 1000
```

**What it does:**
- Real S3 PUT operations
- Actual manifest sorting
- Real catalog commit operations
- Network latency included

### 4. Column Families (Peter Vary's Proposal) 🆕

**Measures:** Column stitching performance with vertical data partitioning

```bash
java -jar target/iceberg-benchmarks.jar \
  --benchmark ColumnFamilies \
  --s3-bucket iceberg-v4-benchmarks \
  --num-files 100
```

**What it does:**
- Tests read performance: single file vs multiple column families
- Tests write performance: monolithic vs parallel column family writes
- Tests partial updates: full rewrite vs column family update
- Tests narrow reads: ML workload (5 columns from 1000)
- Validates if multi-threading can absorb ~10% read overhead

**Reference:** https://github.com/apache/iceberg/pull/13306

See [COLUMN_FAMILIES_BENCHMARK.md](COLUMN_FAMILIES_BENCHMARK.md) for detailed documentation.

## Key Differences from Python Simulations

| Aspect | Python Simulation | Java Real Benchmark |
|--------|------------------|---------------------|
| S3 I/O | `time.sleep(latency)` | Real `S3Client.getObject()` |
| Timing | Python `time.perf_counter()` | JVM `System.nanoTime()` |
| Memory | Python `psutil` | JVM `Runtime.getRuntime()` |
| Operations | Mock file operations | Real Iceberg catalog |
| Network | Simulated latency | Actual network calls |
| GC | Python GC | Real JVM G1GC |

## Expected Results

Based on Apache Iceberg team's internal benchmarks:

- **Delete Storm:** "Hundreds of milliseconds" for 25k entries (not 51 seconds)
- **DV Resolution:** "Percentage points" difference (not 400x)
- **Write Overhead:** Realistic commit times with actual S3 latency

## Cost Considerations

**AWS Costs:**
- S3 storage: ~$0.023/GB/month
- S3 PUT requests: $0.005 per 1,000 requests
- S3 GET requests: $0.0004 per 1,000 requests

**Estimated cost for full benchmark suite:** ~$5-10

## Cleanup

```bash
# Delete S3 bucket and all objects
aws s3 rb s3://iceberg-v4-benchmarks --force

# Or use Terraform
cd terraform
terraform destroy
```

## Results

Results will be saved in `results/` directory with:
- JSON format (machine-readable)
- Markdown reports (human-readable)
- JVM profiling data
- S3 operation logs

## Contributing

This is a work in progress. Feedback welcome on:
- Benchmark methodology
- AWS setup
- Result interpretation

## License

Apache License 2.0 (same as Apache Iceberg)
