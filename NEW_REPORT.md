
Alexandria University  
Faculty of Engineering  
Computers & Systems Engineering Department  
Designing Data Intensive Applications (CSE-4E3)  

# Lab #4 — JMS vs Kafka

**Team members:** `Mohammed Hatem 8995`, `Asser Hanafy 8833`, `Omar Roushdy 9091`, `Moustafa Elnashar 8577`  
**Date:** 2026-05-09

## Test environment

| Item | Value |
|---|---|
| OS | Linux (Ubuntu kernel `6.8.0-111-generic`) |
| CPU | 13th Gen Intel(R) Core(TM) i9-13900H (20 logical CPUs) |
| RAM | 15 GiB |
| Java | OpenJDK 21.0.10 |
| Docker | 29.3.1 |
| ActiveMQ | 5.18.3 (embedded broker via `vm://localhost`) |
| Kafka | Local broker via Docker Compose on `localhost:9092` |
| Message size | 1 KB |

---

## 1) Overview

Message queues are a core building block in data‑intensive applications. They decouple producers from consumers, improve system responsiveness, and enable asynchronous workflows.

- **JMS** is a Java messaging API specification commonly used with brokers such as **Apache ActiveMQ**. It targets enterprise messaging patterns (queues/topics, selectors, request/reply, transactions).
- **Apache Kafka** is a distributed event log and stream-processing platform. It is optimized for high throughput, retention/replay, and integration with stream processing and data pipeline ecosystems.

This report compares **JMS (ActiveMQ embedded)** vs **Kafka (local broker on localhost:9092)** across performance, usability, and integrations, based on the provided benchmark outputs.

**Fairness note:** the JMS run uses an in-process embedded broker (`vm://`), while Kafka always uses TCP. For a stricter apples-to-apples comparison, JMS can also be run in TCP mode (as described in `RUN_AND_REPORT.md`).

---

## 2) Installation & Prerequisites

To reduce moving parts and keep the comparison controlled, the experiments fix:

- **Message size:** 1 KB (`"A".repeat(1024)`)  
- **Programming API:** Java  

Kafka broker is assumed to be started via Docker Compose (as provided in this repo). JMS uses an embedded ActiveMQ broker in **vm://** mode.



---

## A) Performance Comparison

### A.1 Response Time (produce + consume API calls)

#### Methodology

- Produce: execute **1000 produce calls** in a tight loop and time **each call**.
- Consume: execute **1000 consume calls** (one message per call) and time **each call**.
- Report **median** response time (robust to outliers like GC pauses).

#### JMS sample code (from `JMS/src/main/java/jms/ResponseTimeBenchmark.java`)

```java
long start = System.nanoTime();
producer.send(msg);
long produceUs = (System.nanoTime() - start) / 1000;

long start2 = System.nanoTime();
Message m = consumer.receive(5000);
long consumeUs = (System.nanoTime() - start2) / 1000;
```

#### Kafka sample code (from `Kafka/src/main/java/kafka/ResponseTimeBenchmark.java`)

```java
long start = System.nanoTime();
producer.send(record).get();
long produceUs = (System.nanoTime() - start) / 1000;

long start2 = System.nanoTime();
ConsumerRecords<String,String> recs = consumer.poll(Duration.ofSeconds(5));
long consumeUs = (System.nanoTime() - start2) / 1000;
```

#### Results (median of 1000 runs)

| API call | JMS (median) | Kafka (median) |
|---|---:|---:|
| Produce | 18 µs | 2335 µs |
| Consume | 20 µs | 83 µs |

#### Observations

- **JMS (embedded vm://)** has no network hop; the broker runs in-process, so per-call timings are in the tens of microseconds in this run.
- **Kafka (TCP localhost)** must do a network round-trip and wait for broker acknowledgement (`acks=1`), so the produce call median is higher (≈2.3 ms).
- Kafka consume per-call median is low here (83 µs) because polling a local broker with readily available records is cheap, but note the **max** consume time in the raw output shows occasional long pauses (outliers).

---

### A.2 Maximum Throughput (produce + consume)

#### Methodology

For each target throughput $X$ (msg/sec):

1. Compute period $T = 1000/X$ ms.
2. Send/receive messages in a loop for ~1 second, sleeping approximately $0.8T$ after each call (to spread calls across the second).
3. A trial **passes** only if:
   - errors == 0, and
   - all produced messages are successfully drained/consumed.
4. Increase $X$ exponentially (×2) until failure; report the last successful $X$.

Kafka is also validated with the official Kafka perf scripts (which use Kafka-native batching and are not 1:1 comparable to per-call synchronous timings).

#### Results

| API call | JMS max (Java benchmark) | Kafka max (Java “fair vs JMS”) | Kafka max (native perf scripts) |
|---|---:|---:|---:|
| Produce | 204800 msg/sec | 512000 msg/sec | 47431.58 msg/sec |
| Consume | 102400 msg/sec | 102400 msg/sec | 127833.38 msg/sec |

**Notes on Kafka “native perf scripts” values:**
- Producer perf: 1,000,000 records at **47,431.58 records/sec**.
- Consumer perf: 1,000,424 records at **127,833.38 records/sec**.

#### Observations

- **JMS (embedded)** reaches **204,800 msg/sec** produce and **102,400 msg/sec** consume in this setup.
- **Kafka (Java fair benchmark)** reaches **512,000 msg/sec** on produce and **102,400 msg/sec** on consume in this run.
- **Note:** the Kafka Java throughput benchmark uses async `send()` with a final `flush()` (see `Kafka/src/main/java/kafka/ThroughputBenchmark.java`), even though one printed summary line mentions “sync, no batching”.
- **Kafka native scripts** show high throughput on both sides when Kafka is used in its intended batched mode.

---

### A.3 Median Latency (end-to-end: production → consumption)

#### Methodology

- Start a consumer that is already polling.
- Produce **10,000 messages**, each embedding the send timestamp.
  - JMS: message property `ts` (milliseconds).
  - Kafka: record header `ts` (8 bytes).
- On consumption: latency = `nowMillis - ts`.
- Report the **median** latency over 10,000 messages.

#### JMS sample code (from `JMS/src/main/java/jms/LatencyBenchmark.java`)

```java
TextMessage m = session.createTextMessage(payload);
m.setLongProperty("ts", System.currentTimeMillis());
producer.send(m);

Message m2 = consumer.receive();
long latencyMs = System.currentTimeMillis() - m2.getLongProperty("ts");
```

#### Kafka sample code (from `Kafka/src/main/java/kafka/LatencyBenchmark.java`)

```java
byte[] tsBytes = ByteBuffer.allocate(8).putLong(System.currentTimeMillis()).array();
ProducerRecord<String,String> r = new ProducerRecord<>(topic, null, payload);
r.headers().add("ts", tsBytes);
producer.send(r);

Header h = record.headers().lastHeader("ts");
long ts = ByteBuffer.wrap(h.value()).getLong();
long latencyMs = System.currentTimeMillis() - ts;
```

#### Results

| Metric | JMS | Kafka |
|---|---:|---:|
| Messages measured | 10000 | 10000 |
| Median latency | 94 ms | 2825 ms |
| p95 latency | n/a (not reported) | 2859 ms |
| Min / Max | 11 / 146 ms | 2810 / 2899 ms |

#### Observations

- In these runs, **Kafka end-to-end latency is much higher (≈2.8 s median)** than JMS.
- Based on the code, Kafka’s latency benchmark uses an **async producer with batching** (`linger.ms=5`, `batch.size=16384`). Even with batching, multi-second median latency is unusual for localhost and likely reflects broker/consumer scheduling, polling cadence, or environment load during the run.
- JMS (embedded) shows far lower end-to-end latency in this setup.

---

### Performance Summary (single table)

| Metric | JMS | Kafka |
|---|---:|---:|
| Produce response time (median) | 18 µs | 2335 µs |
| Consume response time (median) | 20 µs | 83 µs |
| Max produce throughput (Java benchmark) | 204800 msg/sec | 512000 msg/sec |
| Max consume throughput (Java benchmark) | 102400 msg/sec | 102400 msg/sec |
| Max throughput (Kafka native scripts) | n/a | Produce 47431.58 msg/sec / Consume 127833.38 msg/sec |
| Median end-to-end latency | 94 ms | 2825 ms |
| p95 end-to-end latency | n/a | 2859 ms |

---

## B) Usability

### B.1 Tool setup overhead

| Step | JMS (ActiveMQ embedded) | Kafka (Docker broker) |
|---|---|---|
| Install runtime | JDK + Maven | Docker + Docker Compose + JDK + Maven |
| Broker install | None (embedded broker in-process) | `docker compose up -d` |
| Broker config | Minimal (URL `vm://...`) | Compose YAML + broker env vars |
| Start/stop broker | Automatic with the Java process | Start/stop Docker containers |

**Time to “Hello World”:** not directly logged in the provided outputs; fill in based on your actual setup run if required by your submission template.

### B.2 Degree of code cluttering

This section uses the concrete benchmark code in this repository.

#### Produce path (response-time benchmark)

- **JMS distinct API chain (from `jms.JMSHelper` + `jms.ResponseTimeBenchmark`)**:
  - `new ActiveMQConnectionFactory(...)`
  - `factory.createConnection()`
  - `connection.start()`
  - `connection.createSession(...)`
  - `session.createQueue(...)`
  - `session.createProducer(...)`
  - `producer.setDeliveryMode(...)`
  - `session.createTextMessage(...)`
  - `producer.send(...)`
  - **≈ 9 distinct API calls** end-to-end.

- **Kafka distinct API chain (from `kafka.KafkaHelper` + `kafka.ResponseTimeBenchmark`)**:
  - `new KafkaProducer<>(props)`
  - `new ProducerRecord<>(...)`
  - `producer.send(...).get()`
  - **≈ 3 distinct API calls** end-to-end (excluding property bag construction).

#### Consume path (response-time benchmark)

- **JMS**: `session.createConsumer(queue)` → `consumer.receive(timeout)` → cast/extract if needed.
- **Kafka**: `consumer.subscribe(...)` → `consumer.poll(...)` → iterate records.

**Overall:** JMS is more verbose due to the Connection/Session/Producer/Consumer object model and checked exceptions; Kafka is simpler at the call site but requires more operational concepts (topics, consumer groups, offsets).

---

## C) Integrations

### C.1 Approach (research methodology)

We focused on official project documentation and widely used ecosystem tools, and summarized:

- supported programming languages / client libraries,
- out-of-the-box connector ecosystems for data-intensive applications,
- typical integration paths with databases, object stores, and analytics systems.

### C.2 Programming languages

- **JMS** is a Java specification (Jakarta Messaging). Non-Java languages interoperate via broker-specific protocols (e.g., AMQP/STOMP/MQTT for some brokers), but that is not “JMS across languages”.
- **Kafka** has a stable wire protocol and widely used clients across many languages (Java official; high-quality clients exist for Python, Go, .NET, C/C++, Node.js, Rust, etc.).

**Verdict:** Kafka is polyglot by design; JMS is Java-first by definition.

### C.3 Out-of-the-box external data source integrations

- **Kafka:** Kafka Connect provides a standard connector framework; the ecosystem (e.g., Confluent Hub) includes connectors for CDC (Debezium), JDBC, S3/GCS/Azure Blob, Elasticsearch/OpenSearch, Snowflake, BigQuery, etc.
- **JMS:** There is no direct “JMS Connect” equivalent. Integrations are typically built via:
  - Apache Camel routes,
  - Spring Integration,
  - custom adapters per broker/vendor.

**Verdict:** Kafka has a much larger ready-made integration catalogue for data pipelines.

### References

- Kafka documentation: https://kafka.apache.org/documentation/
- Kafka Connect overview: https://kafka.apache.org/documentation/#connect
- Confluent Hub connectors: https://www.confluent.io/hub/
- Debezium (CDC): https://debezium.io/
- ActiveMQ: https://activemq.apache.org/
- Jakarta Messaging (JMS) spec: https://jakarta.ee/specifications/messaging/
- Apache Camel components: https://camel.apache.org/components/

---

## Summary — JMS (ActiveMQ embedded)

### Advantages

- Lowest per-call response times in this lab setup (tens of µs produce/consume in our run).
- Lower end-to-end latency than Kafka in the provided runs.
- Enterprise messaging patterns are built-in (queues/topics, selectors, request/reply, TTL/priority patterns depending on broker).

### Disadvantages

- Java-centric; cross-language usage is broker-protocol specific, not JMS.
- More verbose API and more checked-exception boilerplate.
- Scaling model depends on broker/vendor; not designed as a distributed event log.

---

## Summary — Kafka

### Advantages

- Very high throughput when used with batching and async producer patterns.
- Replay/retention and stream-processing ecosystem are first-class.
- Strong polyglot client ecosystem.
- Rich connector ecosystem for data-intensive integrations.

### Disadvantages

- Higher per-call produce response time than embedded JMS.
- More operational concepts (topics/partitions/consumer groups/offsets).
- In the provided latency run, end-to-end latency was high (≈2.8 s median), which may require tuning and/or environment investigation.

---

## Conclusion (recommendation)

For a **data-intensive application** (pipelines, stream processing, integration with external stores, replay/retention), **Kafka** is generally the better strategic choice due to its ecosystem, scaling model, and connector availability.

For **ultra-low-latency in-process Java messaging** (especially where a broker is acceptable inside the same JVM/process for lab or tightly coupled deployment), **JMS/ActiveMQ embedded** shows dramatically lower per-call latency and lower end-to-end latency in these runs.

**Recommendation for this lab framing:** Kafka, with a note that the Kafka latency benchmark result should be validated/tuned if low-latency is a hard requirement.

---

## Appendix — Raw summary blocks (from `results_*.txt`)

### JMS: response time

```text
=== SUMMARY ===
  Produce response time (median): 18 µs
  Consume response time (median): 20 µs
```

### JMS: throughput

```text
=== SUMMARY ===
  Max Produce throughput: 204800 msg/sec
  Max Consume throughput: 102400 msg/sec
```

### JMS: latency

```text
=== SUMMARY ===
  Messages measured : 10000
  Median latency    : 94 ms
  Min latency       : 11 ms
  Max latency       : 146 ms
```

### Kafka: response time

```text
=== SUMMARY (copy to report) ===
  Kafka produce response time (median): 2335 µs
  Kafka consume response time (median): 83 µs
```

### Kafka: throughput

```text
=== SUMMARY (copy to report) ===
  Kafka max produce throughput (Java, sync, no batching): 512000 msg/sec
  Kafka max consume throughput (Java, max.poll.records=1): 102400 msg/sec
```

### Kafka: latency

```text
=== SUMMARY (copy to report) ===
  Messages measured       : 10000
  Kafka median latency    : 2825 ms
  Kafka min latency       : 2810 ms
  Kafka max latency       : 2899 ms
  Kafka p95 latency       : 2859 ms
```
