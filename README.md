# NATS Benchmark

A comprehensive benchmarking tool for NATS, written in Rust. Supports KV (Key-Value) and JetStream operations with detailed performance metrics.

## Features

- **Multiple Benchmark Modes**:
  - KV PUT operations
  - KV GET operations
  - KV Mixed workloads (configurable read/write ratio)
  - JetStream Publish operations
  - JetStream Consume operations
  - JetStream Pub/Sub operations

- **Flexible Configuration**:
  - Configurable number of worker threads
  - Variable message sizes
  - Duration or count-based benchmarks
  - Rate limiting support
  - Batch operations

- **Detailed Metrics**:
  - Throughput (ops/sec, MB/sec)
  - Latency percentiles (p50, p90, p95, p99, p99.9)
  - Per-worker statistics
  - Success/failure rates
  - Real-time progress updates

- **Multiple Output Formats**:
  - Text (human-readable with formatting)
  - JSON (machine-readable)
  - CSV (for analysis)

## Installation

### From Source

```bash
git clone <repository-url>
cd nats-benchmark
cargo build --release
```

The binary will be available at `target/release/nats-benchmark`.

## Usage

### Basic Examples

#### KV PUT Benchmark

```bash
# Run 10 threads for 60 seconds with 1KB messages
nats-benchmark kv-put \
  --url nats://localhost:4222 \
  --threads 10 \
  --message-size 1024 \
  --duration 60s \
  --bucket my-bucket
```

#### KV GET Benchmark

```bash
# Run 8 threads, fetch 1 million messages
nats-benchmark kv-get \
  --url nats://localhost:4222 \
  --threads 10 \
  --count 1000000 \
  --bucket my-bucket
```

#### KV Mixed Workload

```bash
# 70% reads, 30% writes
nats-benchmark kv-mixed \
  --url nats://localhost:4222 \
  --threads 10 \
  --duration 60s \
  --read-ratio 0.7 \
  --bucket my-bucket
```

#### JetStream Publish

```bash
# Publish with 20 threads, wait for acks
nats-benchmark js-publish \
  --url nats://localhost:4222 \
  --threads 10 \
  --message-size 1024 \
  --duration 60s \
  --stream BENCHMARK \
  --subject benchmark.test \
  --wait-for-ack
```

#### JetStream Consume

```bash
# Consume messages with 5 threads
nats-benchmark js-consume \
  --url nats://localhost:4222 \
  --threads 10 \
  --duration 60s \
  --stream BENCHMARK \
  --subject benchmark.test \
  --consumer benchmark-consumer
```

#### JetStream Pub/Sub

```bash
# 10 publishers, 5 subscribers
nats-benchmark js-pubsub \
  --url nats://localhost:4222 \
  --pub-threads 5 \
  --sub-threads 5 \
  --duration 60s \
  --stream BENCHMARK \
  --subject benchmark.test
```

### Advanced Options

#### Rate Limiting

```bash
# Limit to 10,000 operations per second
nats-benchmark kv-put \
  --threads 10 \
  --rate 10000 \
  --duration 60s
```

#### Custom Key Distribution

```bash
# Use 10,000 unique keys
nats-benchmark kv-put \
  --num-keys 10000 \
  --key-prefix myapp \
  --duration 60s
```

#### Multiple Output Formats

```bash
# JSON output
nats-benchmark kv-put --duration 30s --output json > results.json

# CSV output
nats-benchmark kv-put --duration 30s --output csv > results.csv

# Text output (default)
nats-benchmark kv-put --duration 30s --output text
```

#### Verbose Logging

```bash
# Enable debug logging
nats-benchmark kv-put --verbose --duration 30s
```

## Configuration Options

### Global Options

- `--url <URL>`: NATS server URL (default: `nats://localhost:4222`)
- `--verbose, -v`: Enable verbose logging
- `--output <FORMAT>`: Output format: text, json, csv (default: text)

### KV Options

- `--threads, -t <N>`: Number of worker threads (default: 1)
- `--message-size, -m <BYTES>`: Message size in bytes (default: 1024)
- `--duration, -d <DURATION>`: Run duration (e.g., 30s, 5m)
- `--count, -c <N>`: Number of operations to perform
- `--bucket, -b <NAME>`: KV bucket name (default: benchmark)
- `--key-prefix <PREFIX>`: Key prefix (default: key)
- `--num-keys <N>`: Number of unique keys to use (default: 1000)
- `--rate, -r <N>`: Rate limit in ops/sec (0 = unlimited, default: 0)

### KV Mixed Options

- All KV options plus:
- `--read-ratio <RATIO>`: Read ratio from 0.0 to 1.0 (default: 0.5)

### JetStream Options

- `--threads, -t <N>`: Number of worker threads (default: 1)
- `--message-size, -m <BYTES>`: Message size in bytes (default: 1024)
- `--duration, -d <DURATION>`: Run duration (e.g., 30s, 5m)
- `--count, -c <N>`: Number of operations to perform
- `--stream <NAME>`: Stream name (default: BENCHMARK)
- `--subject <SUBJECT>`: Subject for pub/sub (default: benchmark.test)
- `--consumer <NAME>`: Consumer name (default: benchmark-consumer)
- `--batch-size <N>`: Batch size for operations (default: 1)
- `--rate, -r <N>`: Rate limit in ops/sec (0 = unlimited, default: 0)
- `--wait-for-ack`: Wait for acknowledgment on publish

### JetStream Pub/Sub Options

- All JetStream options plus:
- `--pub-threads <N>`: Number of publisher threads (default: 1)
- `--sub-threads <N>`: Number of subscriber threads (default: 1)

## Output

### Text Format

The text format provides a detailed, human-readable report:

```
================================================================================
NATS Benchmark Results - KV PUT
================================================================================

📊 Overall Statistics:
  Duration:              60.02s
  Total Operations:      1000000
  Successful:            1000000
  Failed:                0
  Success Rate:          100.00%

🚀 Throughput:
  Operations/sec:        16661.11
  Throughput:            16.27 MB/s
  Data Transferred:      976.56 MB

⏱️  Latency (microseconds):
  Min:                   45
  Max:                   12456
  Mean:                  599.23
  StdDev:                234.56
  p50:                   567
  p90:                   845
  p95:                   967
  p99:                   1234
  p99.9:                 2345

👷 Worker Statistics:
  Worker   Success         Failed          Data
  ------------------------------------------------------------
  0        100000          0               97.66 MB
  1        100000          0               97.66 MB
  ...
```

### JSON Format

Structured output for programmatic processing:

```json
{
  "duration_secs": 60.02,
  "total_operations": 1000000,
  "successful_operations": 1000000,
  "failed_operations": 0,
  "operations_per_sec": 16661.11,
  "bytes_transferred": 1024000000,
  "throughput_mb_per_sec": 16.27,
  "latency_stats": {
    "min_micros": 45,
    "max_micros": 12456,
    "mean_micros": 599.23,
    "stddev_micros": 234.56,
    "p50_micros": 567,
    "p90_micros": 845,
    "p95_micros": 967,
    "p99_micros": 1234,
    "p999_micros": 2345
  },
  "worker_stats": [...]
}
```

### CSV Format

Easy to import into spreadsheets:

```csv
metric,value
duration_secs,60.02
total_operations,1000000
successful_operations,1000000
operations_per_sec,16661.11
...
```

## Requirements

- Rust 1.70 or later
- NATS server (with JetStream enabled for JetStream benchmarks)

## Development

### Building

```bash
cargo build
```

### Running Tests

```bash
cargo test
```

### Running with Debug Logging

```bash
RUST_LOG=debug cargo run -- kv-put --duration 10s
```

## Architecture

The project is organized into several modules:

- `config.rs`: CLI argument parsing and configuration structures
- `metrics.rs`: Performance metrics collection and statistics
- `worker.rs`: Worker pool management and rate limiting
- `benchmarks/`: Benchmark implementations
  - `mod.rs`: Common benchmark trait
  - `kv.rs`: KV benchmark implementations
  - `jetstream.rs`: JetStream benchmark implementations
- `reporter.rs`: Results formatting and output
- `main.rs`: CLI entry point

## Performance Tips

1. **Thread Count**: Start with `--threads` equal to your CPU cores
2. **Rate Limiting**: Use `--rate` to avoid overwhelming the server
3. **Message Size**: Larger messages = higher throughput but lower ops/sec
4. **Duration vs Count**: Use `--duration` for steady-state testing, `--count` for fixed workloads
5. **Warm-up**: Run a short benchmark first to warm up connections

## License

MIT

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.
