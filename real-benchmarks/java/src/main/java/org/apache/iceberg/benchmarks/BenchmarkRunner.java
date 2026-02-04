package org.apache.iceberg.benchmarks;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Main entry point for Apache Iceberg V4 Real Benchmarks
 * 
 * Usage:
 *   java -jar iceberg-v4-benchmarks.jar \
 *     --benchmark DeleteStorm \
 *     --s3-bucket iceberg-v4-benchmarks \
 *     --num-files 10000
 * 
 * Available benchmarks:
 *   - DeleteStorm: Inline vs External DVs with real S3 I/O
 *   - DVResolution: Hash vs Positional joins with real memory profiling
 *   - WriteOverhead: Sorted vs Unsorted writes with real commits
 */
public class BenchmarkRunner {
    private static final Logger LOG = LoggerFactory.getLogger(BenchmarkRunner.class);
    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

    public static void main(String[] args) {
        LOG.info("=".repeat(70));
        LOG.info("Apache Iceberg V4 Real Benchmarks");
        LOG.info("Using: Real AWS S3 + JVM + Actual Iceberg Operations");
        LOG.info("=".repeat(70));

        // Parse command line arguments
        BenchmarkConfig config = parseArgs(args);
        
        if (config == null) {
            printUsage();
            System.exit(1);
        }

        // Validate AWS credentials
        if (!validateAWSCredentials()) {
            LOG.error("AWS credentials not configured. Run: aws configure");
            System.exit(1);
        }

        // Run the specified benchmark
        BenchmarkResult result = null;
        try {
            switch (config.benchmarkType) {
                case "DeleteStorm":
                    result = runDeleteStormBenchmark(config);
                    break;
                case "DVResolution":
                    result = runDVResolutionBenchmark(config);
                    break;
                case "WriteOverhead":
                    result = runWriteOverheadBenchmark(config);
                    break;
                default:
                    LOG.error("Unknown benchmark: {}", config.benchmarkType);
                    printUsage();
                    System.exit(1);
            }

            // Save results
            saveResults(result, config);
            
            // Print summary
            printSummary(result);

        } catch (Exception e) {
            LOG.error("Benchmark failed", e);
            System.exit(1);
        }

        LOG.info("\n✅ Benchmark complete!");
    }

    private static BenchmarkResult runDeleteStormBenchmark(BenchmarkConfig config) {
        LOG.info("\n" + "=".repeat(70));
        LOG.info("Running: Delete Storm Benchmark");
        LOG.info("Measuring: Inline vs External DVs with real S3 I/O");
        LOG.info("=".repeat(70));

        DeleteStormBenchmark benchmark = new DeleteStormBenchmark(config);
        return benchmark.run();
    }

    private static BenchmarkResult runDVResolutionBenchmark(BenchmarkConfig config) {
        LOG.info("\n" + "=".repeat(70));
        LOG.info("Running: DV Resolution Benchmark");
        LOG.info("Measuring: Hash vs Positional joins with real memory profiling");
        LOG.info("=".repeat(70));

        DVResolutionBenchmark benchmark = new DVResolutionBenchmark(config);
        return benchmark.run();
    }

    private static BenchmarkResult runWriteOverheadBenchmark(BenchmarkConfig config) {
        LOG.info("\n" + "=".repeat(70));
        LOG.info("Running: Write Overhead Benchmark");
        LOG.info("Measuring: Sorted vs Unsorted writes with real commits");
        LOG.info("=".repeat(70));

        WriteOverheadBenchmark benchmark = new WriteOverheadBenchmark(config);
        return benchmark.run();
    }

    private static BenchmarkConfig parseArgs(String[] args) {
        BenchmarkConfig config = new BenchmarkConfig();
        
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--benchmark":
                    config.benchmarkType = args[++i];
                    break;
                case "--s3-bucket":
                    config.s3Bucket = args[++i];
                    break;
                case "--num-files":
                    config.numFiles = Integer.parseInt(args[++i]);
                    break;
                case "--num-commits":
                    config.numCommits = Integer.parseInt(args[++i]);
                    break;
                case "--aws-region":
                    config.awsRegion = args[++i];
                    break;
                case "--output-dir":
                    config.outputDir = args[++i];
                    break;
                case "--help":
                    return null;
                default:
                    LOG.warn("Unknown argument: {}", args[i]);
            }
        }

        // Validate required fields
        if (config.benchmarkType == null || config.s3Bucket == null) {
            return null;
        }

        return config;
    }

    private static boolean validateAWSCredentials() {
        try {
            // Try to create S3 client - will fail if credentials not configured
            software.amazon.awssdk.services.s3.S3Client.builder().build();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static void saveResults(BenchmarkResult result, BenchmarkConfig config) {
        String filename = String.format("%s/%s_results_%s.json",
                config.outputDir,
                config.benchmarkType.toLowerCase(),
                Instant.now().toString().replace(":", "-"));

        try (FileWriter writer = new FileWriter(filename)) {
            GSON.toJson(result, writer);
            LOG.info("\n✅ Results saved to: {}", filename);
        } catch (IOException e) {
            LOG.error("Failed to save results", e);
        }
    }

    private static void printSummary(BenchmarkResult result) {
        LOG.info("\n" + "=".repeat(70));
        LOG.info("BENCHMARK SUMMARY");
        LOG.info("=".repeat(70));
        LOG.info("Benchmark: {}", result.benchmarkName);
        LOG.info("Duration: {} ms", result.totalDurationMs);
        LOG.info("Files processed: {}", result.filesProcessed);
        LOG.info("S3 operations: {}", result.s3Operations);
        LOG.info("=".repeat(70));
    }

    private static void printUsage() {
        System.out.println("\nUsage:");
        System.out.println("  java -jar iceberg-v4-benchmarks.jar [OPTIONS]");
        System.out.println("\nRequired Options:");
        System.out.println("  --benchmark <type>      Benchmark to run (DeleteStorm, DVResolution, WriteOverhead)");
        System.out.println("  --s3-bucket <name>      S3 bucket name for test data");
        System.out.println("\nOptional:");
        System.out.println("  --num-files <n>         Number of files to test (default: 10000)");
        System.out.println("  --num-commits <n>       Number of commits to test (default: 1000)");
        System.out.println("  --aws-region <region>   AWS region (default: us-east-1)");
        System.out.println("  --output-dir <path>     Output directory (default: ./results)");
        System.out.println("\nExamples:");
        System.out.println("  java -jar iceberg-v4-benchmarks.jar --benchmark DeleteStorm --s3-bucket my-bucket --num-files 25000");
        System.out.println("  java -jar iceberg-v4-benchmarks.jar --benchmark DVResolution --s3-bucket my-bucket");
        System.out.println();
    }

    /**
     * Configuration for benchmark execution
     */
    public static class BenchmarkConfig {
        public String benchmarkType;
        public String s3Bucket;
        public int numFiles = 10000;
        public int numCommits = 1000;
        public String awsRegion = "us-east-1";
        public String outputDir = "./results";
    }

    /**
     * Result from benchmark execution
     */
    public static class BenchmarkResult {
        public String benchmarkName;
        public long totalDurationMs;
        public int filesProcessed;
        public int s3Operations;
        public Map<String, Object> metrics = new HashMap<>();
        public String timestamp = Instant.now().toString();
    }
}
