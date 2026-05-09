# JMS Benchmark — Member 2

Lab 4: JMS vs Kafka | CSE-4E3, Alexandria University  
**Your responsibility**: ActiveMQ setup + JMS produce/consume code + all 3 performance metrics.

---

## Quick Start (Ubuntu 24.04)

```bash
# 1. Clone / copy this folder onto your machine
# 2. Make the runner executable
chmod +x run_all.sh

# 3. Run everything (installs ActiveMQ, builds, runs all benchmarks)
./run_all.sh
```

Results are written to `results_response_time.txt`, `results_throughput.txt`, `results_latency.txt`.

---

## Manual Steps (if you prefer control)

### Install Java & Maven
```bash
sudo apt update
sudo apt install -y default-jdk maven
```

### Download & Start ActiveMQ
```bash
wget https://downloads.apache.org/activemq/5.18.3/apache-activemq-5.18.3-bin.tar.gz
tar -xzf apache-activemq-5.18.3-bin.tar.gz
./apache-activemq-5.18.3/bin/activemq start
```
ActiveMQ web console: http://localhost:8161  (admin / admin)

### Build
```bash
mvn package -DskipTests
```

### Run Each Benchmark Individually
```bash
# Response Time
java -cp target/jms-benchmark-1.0-SNAPSHOT.jar jms.ResponseTimeBenchmark

# Max Throughput
java -cp target/jms-benchmark-1.0-SNAPSHOT.jar jms.ThroughputBenchmark

# Median Latency
java -cp target/jms-benchmark-1.0-SNAPSHOT.jar jms.LatencyBenchmark
```

### Stop ActiveMQ
```bash
./apache-activemq-5.18.3/bin/activemq stop
```

---

## What Each Class Does

| Class | Metric | Lab Protocol |
|---|---|---|
| `ResponseTimeBenchmark.java` | Produce & Consume response time | Median of 1000 runs, 1 KB messages |
| `ThroughputBenchmark.java` | Max produce & consume throughput | Exponential ramp-up (×2), T − 0.2T sleep |
| `LatencyBenchmark.java` | Producer→Consumer median latency | 10 000 messages, timestamp embedded in message property |
| `JMSHelper.java` | Shared broker connection logic | — |

---

## Report Snippet Guide

After running, grep the output for `SUMMARY` lines. They look like:

```
=== SUMMARY (copy to report) ===
  Produce response time (median): X ms
  Consume response time (median): X ms

  Max Produce throughput: X msg/sec
  Max Consume throughput: X msg/sec

  JMS Median latency (producer→consumer): X ms
```

Paste these into Member 4's report template.

---

## Notes for Report (Usability — help for Member 3)

**Setup steps counted**:
1. Install Java
2. Install Maven
3. Download ActiveMQ tarball
4. Extract and start broker
5. Add `activemq-all` Maven dependency
6. Write connection factory + session boilerplate
7. Write producer (3 API calls: createProducer, createTextMessage, send)
8. Write consumer (2 API calls: createConsumer, receive)

**Lines of code** (end-to-end produce): ~15 lines including connection setup  
**Lines of code** (end-to-end consume): ~12 lines including connection setup  
**API calls for produce**: ConnectionFactory → Connection → Session → Queue → MessageProducer → send()  
**API calls for consume**: same setup → MessageConsumer → receive()

This is verbose compared to Kafka's `KafkaProducer` / `KafkaConsumer` which need far fewer boilerplate steps.
