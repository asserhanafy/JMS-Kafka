#!/usr/bin/env bash
# ============================================================================
#  run_all.sh - JMS Benchmark Runner
#
#  Default: uses an *embedded* in-process ActiveMQ broker.
#           No external broker install needed; the JVM starts one when the
#           first ActiveMQConnectionFactory("vm://...") is created.
#
#  Optional: to benchmark against a real TCP broker for more realistic
#            numbers (network hop in the loop), do:
#
#       export BROKER_URL=tcp://localhost:61616
#       # ...start ActiveMQ separately, e.g.:
#       # wget https://archive.apache.org/dist/activemq/5.18.3/apache-activemq-5.18.3-bin.tar.gz
#       # tar -xzf apache-activemq-5.18.3-bin.tar.gz
#       # ./apache-activemq-5.18.3/bin/activemq start
#       ./run_all.sh
#       # ./apache-activemq-5.18.3/bin/activemq stop
# ============================================================================
set -e

echo ">>> JMS benchmark - broker URL: ${BROKER_URL:-vm://localhost?broker.persistent=false (embedded)}"
echo ""

# --- Build -----------------------------------------------------------------
echo ">>> Compiling..."
mvn -q compile

# Build classpath = our compiled classes + every dependency JAR Maven knows about
CP="target/classes:$(mvn -q dependency:build-classpath -Dmdep.outputFile=/dev/stdout 2>/dev/null)"
echo ">>> Classpath built."
echo ""

JAVA_OPTS="-Dlog4j2.level=WARN"

# --- Run benchmarks --------------------------------------------------------
echo "================================================================"
echo "  BENCHMARK 1/3 : Response Time"
echo "================================================================"
java $JAVA_OPTS -cp "$CP" jms.ResponseTimeBenchmark 2>&1 | tee results_response_time.txt

echo ""
echo "================================================================"
echo "  BENCHMARK 2/3 : Maximum Throughput"
echo "================================================================"
java $JAVA_OPTS -cp "$CP" jms.ThroughputBenchmark 2>&1 | tee results_throughput.txt

echo ""
echo "================================================================"
echo "  BENCHMARK 3/3 : Median Latency"
echo "================================================================"
java $JAVA_OPTS -cp "$CP" jms.LatencyBenchmark 2>&1 | tee results_latency.txt

echo ""
echo "================================================================"
echo "  ALL DONE. Results files:"
echo "    JMS/results_response_time.txt"
echo "    JMS/results_throughput.txt"
echo "    JMS/results_latency.txt"
echo "  Look for the '=== SUMMARY ===' lines and copy into the report."
echo "================================================================"
