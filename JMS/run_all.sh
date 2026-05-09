#!/usr/bin/env bash
# ================================================================
# run_all.sh - JMS Benchmark Runner (Ubuntu 24.04)
# ================================================================
set -e

# ── Step 1: Check Java & Maven ───────────────────────────────────
echo ">>> Checking Java & Maven..."
java -version
mvn -version

# ── Step 2: Start ActiveMQ ───────────────────────────────────────
echo ""
echo ">>> Starting ActiveMQ broker..."
sudo systemctl start activemq || sudo activemq start || true
sleep 5
echo ">>> Broker status:"
sudo systemctl status activemq --no-pager 2>/dev/null || true

# ── Step 3: Build (compile only, no shade) ───────────────────────
echo ""
echo ">>> Compiling..."
mvn -q compile

# Classpath = our compiled classes + all dependency JARs from local repo
CP="target/classes:$(mvn -q dependency:build-classpath -Dmdep.outputFile=/dev/stdout 2>/dev/null)"
echo ">>> Classpath built."

# ── Step 4: Run benchmarks ───────────────────────────────────────
echo ""
echo "================================================================"
echo "  BENCHMARK 1: Response Time"
echo "================================================================"
java -cp "$CP" jms.ResponseTimeBenchmark 2>&1 | tee results_response_time.txt

echo ""
echo "================================================================"
echo "  BENCHMARK 2: Maximum Throughput"
echo "================================================================"
java -cp "$CP" jms.ThroughputBenchmark 2>&1 | tee results_throughput.txt

echo ""
echo "================================================================"
echo "  BENCHMARK 3: Median Latency"
echo "================================================================"
java -cp "$CP" jms.LatencyBenchmark 2>&1 | tee results_latency.txt

# ── Step 5: Stop broker ──────────────────────────────────────────
echo ""
echo ">>> Stopping ActiveMQ..."
sudo systemctl stop activemq 2>/dev/null || sudo activemq stop || true

echo ""
echo "================================================================"
echo "  ALL DONE."
echo "  results_response_time.txt"
echo "  results_throughput.txt"
echo "  results_latency.txt"
echo "================================================================"