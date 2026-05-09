#!/usr/bin/env bash
# ============================================================================
#  run_all.sh - Kafka Benchmark Runner
#
#  Pre-req: docker compose up -d   (starts Kafka in KRaft mode on :9092)
#
#  Runs three Java benchmarks (response time / throughput / latency) that
#  use the SAME methodology as the JMS benchmarks for fair comparison.
#  Also runs the official kafka-producer-perf-test.sh / kafka-consumer-perf-test.sh
#  which the lab handout mentions, as a Kafka-native sanity check.
# ============================================================================
set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
COMPOSE_FILE="$SCRIPT_DIR/../docker-compose.yml"

KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP:-localhost:9092}
echo ">>> Kafka benchmark - bootstrap: $KAFKA_BOOTSTRAP"
echo ""

echo ">>> Starting Kafka via docker compose..."
docker compose -f "$COMPOSE_FILE" up -d kafka >/dev/null

# --- Wait for broker to be reachable ---------------------------------------
echo ">>> Waiting for Kafka to accept connections..."
for i in {1..30}; do
    if docker compose -f "$COMPOSE_FILE" ps kafka 2>/dev/null | grep -q "Up"; then
        if docker compose -f "$COMPOSE_FILE" exec -T kafka \
                            /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
            echo ">>> Broker is up."
            break
        fi
    fi
    sleep 2
    if [ "$i" = "30" ]; then
        echo "ERROR: Kafka not reachable. Did you run 'docker compose up -d'?"
        exit 1
    fi
done
echo ""

# --- Build -----------------------------------------------------------------
echo ">>> Compiling..."
mvn -q compile

CP="target/classes:$(mvn -q dependency:build-classpath -Dmdep.outputFile=/dev/stdout 2>/dev/null)"
echo ">>> Classpath built."
echo ""

JAVA_OPTS="-Dorg.slf4j.simpleLogger.defaultLogLevel=warn"

# --- Run Java benchmarks (fair comparison vs JMS) --------------------------
echo "================================================================"
echo "  BENCHMARK 1/3 : Response Time (Java, same shape as JMS)"
echo "================================================================"
java $JAVA_OPTS -cp "$CP" kafka.ResponseTimeBenchmark 2>&1 | tee results_response_time.txt

echo ""
echo "================================================================"
echo "  BENCHMARK 2/3 : Maximum Throughput (Java, fair vs JMS)"
echo "================================================================"
java $JAVA_OPTS -cp "$CP" kafka.ThroughputBenchmark 2>&1 | tee results_throughput.txt

echo ""
echo "================================================================"
echo "  BENCHMARK 3/3 : Median Latency"
echo "================================================================"
java $JAVA_OPTS -cp "$CP" kafka.LatencyBenchmark 2>&1 | tee results_latency.txt

# --- Optional: official Kafka perf-test scripts (Kafka-native) -------------
echo ""
echo "================================================================"
echo "  EXTRA : Official kafka-producer-perf-test.sh (batched, native)"
echo "================================================================"
docker compose -f "$COMPOSE_FILE" exec -T kafka \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
        --create --if-not-exists --topic perf-test --partitions 1 --replication-factor 1 \
        2>&1 | tee -a results_throughput.txt || true

docker compose -f "$COMPOSE_FILE" exec -T kafka \
    /opt/kafka/bin/kafka-producer-perf-test.sh \
        --topic perf-test \
        --num-records 1000000 \
        --record-size 1000 \
        --throughput -1 \
        --producer-props bootstrap.servers=localhost:9092 acks=1 \
    2>&1 | tee -a results_throughput.txt

echo ""
echo "================================================================"
echo "  EXTRA : Official kafka-consumer-perf-test.sh"
echo "================================================================"
docker compose -f "$COMPOSE_FILE" exec -T kafka \
    /opt/kafka/bin/kafka-consumer-perf-test.sh \
        --bootstrap-server localhost:9092 \
        --topic perf-test \
        --messages 1000000 \
    2>&1 | tee -a results_throughput.txt

echo ""
echo "================================================================"
echo "  ALL DONE. Results files:"
echo "    Kafka/results_response_time.txt"
echo "    Kafka/results_throughput.txt    (Java + official perf-test scripts)"
echo "    Kafka/results_latency.txt"
echo "  Look for the '=== SUMMARY ===' lines and copy into the report."
echo "================================================================"
