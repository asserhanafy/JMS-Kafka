# Lab 4 - JMS vs Kafka

**Course:** CSE-4E3 Designing Data Intensive Applications
**Faculty of Engineering, Alexandria University**

This folder is a complete, self-contained solution to Lab 4. It contains:

```
Lab4-Solution/
├── README.md              <-- you are here. Index file.
├── RUN_AND_REPORT.md      <-- "How do I actually run this and write the report?"
├── EXPLANATION.md         <-- "What is going on conceptually + technically?
│                              What will the TA ask me?"
├── JMS/                   <-- ActiveMQ + JMS Java benchmarks (3 metrics)
│   ├── pom.xml
│   ├── run_all.sh
│   └── src/main/java/jms/
│       ├── JMSHelper.java
│       ├── ResponseTimeBenchmark.java
│       ├── ThroughputBenchmark.java
│       └── LatencyBenchmark.java
└── Kafka/                 <-- Apache Kafka (KRaft, Docker) + Java benchmarks
    ├── pom.xml
    ├── run_all.sh
    ├── docker-compose.yml
    └── src/main/java/kafka/
        ├── KafkaHelper.java
        ├── ResponseTimeBenchmark.java
        ├── ThroughputBenchmark.java
        └── LatencyBenchmark.java
```

## What each top-level file is for

| File | Purpose |
|---|---|
| `RUN_AND_REPORT.md` | Step-by-step "press these buttons, then paste these numbers into the report". Includes the full report skeleton with `<PLACEHOLDERS>` you fill in once benchmarks finish. |
| `EXPLANATION.md` | Reading material. The conceptual layer first (what is a message queue, JMS vs Kafka philosophy), then the technical layer (broker internals, consumer groups, ack modes, partitions), then a "TA Q&A" section with answers to the predictable discussion questions. Read this *before* the discussion. |
| `JMS/` | Java code that talks to ActiveMQ over JMS. Default mode uses an *embedded* in-process broker (no install needed). TCP mode also supported. |
| `Kafka/` | Java code that talks to Apache Kafka. The broker is brought up via Docker Compose with one command - KRaft mode, no Zookeeper. |

## TL;DR - fastest path to a working run

```bash
# JMS half (no install required, embedded broker)
cd JMS
chmod +x run_all.sh && ./run_all.sh

# Kafka half (needs Docker)
cd ../Kafka
docker compose up -d            # starts Kafka in KRaft mode on :9092
chmod +x run_all.sh && ./run_all.sh
docker compose down             # tear down when done
```

Each `run_all.sh` writes three text files of results - copy the `=== SUMMARY ===` lines into the report template in `RUN_AND_REPORT.md`. That's it.

## What was kept, changed, or added vs the reference repo

The reference repo (`asserhanafy/JMS-Kafka`) contained only a JMS scaffold - no Kafka, no report, no concepts doc. Here:

- **JMS code** - kept the structure (`JMSHelper`, three benchmark classes) and the methodology (median of 1000 / exponential ramp-up / 10K timestamped messages). Fixed the broken `run_all.sh` which simultaneously tried to run `systemctl start activemq` and connect via `vm://localhost?broker.persistent=false` (mutually exclusive). Now the script honestly defaults to the embedded broker.
- **Kafka code** - written from scratch so the Java methodology mirrors JMS exactly (same RUNS=1000, same ×2 exponential ramp-up, same 10K latency probe). This gives apples-to-apples numbers. Also wires up the official `kafka-producer-perf-test.sh` / `kafka-consumer-perf-test.sh` as a sanity check, since the lab explicitly mentions them.
- **Docs** - `EXPLANATION.md` and `RUN_AND_REPORT.md` are new, written for this lab's deliverables.

Read `RUN_AND_REPORT.md` next.
