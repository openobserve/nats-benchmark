#!/bin/bash
# NATS Benchmark Examples
# Make sure NATS server is running at localhost:4222

echo "=========================================="
echo "NATS Benchmark Tool - Example Usage"
echo "=========================================="
echo ""

# Example 1: KV PUT benchmark
echo "1. KV PUT Benchmark - 10 threads, 1KB messages, 10 seconds"
./target/release/nats-benchmark kv-put \
  --threads 10 \
  --message-size 1024 \
  --duration 10s \
  --bucket benchmark

echo ""
echo "=========================================="
echo ""

# Example 2: KV GET benchmark
echo "2. KV GET Benchmark - 5 threads, 100K operations"
./target/release/nats-benchmark kv-get \
  --threads 5 \
  --count 100000 \
  --bucket benchmark

echo ""
echo "=========================================="
echo ""

# Example 3: KV Mixed workload
echo "3. KV Mixed Benchmark - 70% reads, 30% writes"
./target/release/nats-benchmark kv-mixed \
  --threads 8 \
  --read-ratio 0.7 \
  --duration 10s \
  --bucket benchmark

echo ""
echo "=========================================="
echo ""

# Example 4: JetStream Publish
echo "4. JetStream Publish - 20 threads with acknowledgments"
./target/release/nats-benchmark js-publish \
  --threads 20 \
  --message-size 1024 \
  --duration 10s \
  --stream BENCHMARK \
  --subject benchmark.test \
  --wait-for-ack

echo ""
echo "=========================================="
echo ""

# Example 5: JetStream Consume
echo "5. JetStream Consume - 5 threads"
./target/release/nats-benchmark js-consume \
  --threads 5 \
  --duration 10s \
  --stream BENCHMARK \
  --consumer benchmark-consumer

echo ""
echo "=========================================="
echo ""

# Example 6: JSON output
echo "6. KV PUT with JSON output"
./target/release/nats-benchmark kv-put \
  --threads 5 \
  --duration 5s \
  --output json > benchmark-results.json

echo "Results saved to benchmark-results.json"
cat benchmark-results.json | head -20

echo ""
echo "=========================================="
echo "Examples completed!"
echo "=========================================="
