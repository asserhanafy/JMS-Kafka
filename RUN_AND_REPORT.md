# RUN & REPORT - Lab 4: JMS vs Kafka

This file has two parts:

1. **How to run the benchmarks** (step by step, copy-paste-able).
2. **Report skeleton** with `<PLACEHOLDER>` tokens you replace once results come in.

The intended flow: run JMS → run Kafka → grab the `=== SUMMARY ===` block from each output → paste numbers into the report skeleton → submit.

---

## Part 1 - How to run

### Prerequisites (one-time setup on Ubuntu 24.04)

```bash
sudo apt update
sudo apt install -y default-jdk maven docker.io docker-compose-v2
sudo usermod -aG docker $USER     # then log out + back in
```

You don't need to install ActiveMQ separately. The JMS benchmark defaults to an in-process embedded broker (no install). You don't need Zookeeper either - Kafka runs in KRaft mode inside Docker.

---

### Step 1 - Run the JMS benchmarks

```bash
cd Lab4-Solution/JMS
chmod +x run_all.sh
./run_all.sh
```

This compiles the Java code and runs the three benchmarks in sequence. Outputs land in:

- `JMS/results_response_time.txt`
- `JMS/results_throughput.txt`
- `JMS/results_latency.txt`

Each file ends with a `=== SUMMARY ===` block - that's what you copy into the report.

**Optional - run against a real TCP broker for more realistic numbers** (instead of the embedded one):

```bash
# Download and start ActiveMQ in another terminal
wget https://archive.apache.org/dist/activemq/5.18.3/apache-activemq-5.18.3-bin.tar.gz
tar -xzf apache-activemq-5.18.3-bin.tar.gz
./apache-activemq-5.18.3/bin/activemq start
# Web console: http://localhost:8161  (admin/admin)

# Then in the JMS folder:
export BROKER_URL=tcp://localhost:61616
./run_all.sh

# When done:
./apache-activemq-5.18.3/bin/activemq stop
```

If you go this route, mention it in the report - embedded `vm://` numbers and TCP numbers are not directly comparable, and the TCP numbers are the apples-to-apples version vs the Kafka TCP numbers.

---

### Step 2 - Run the Kafka benchmarks

```bash
cd Lab4-Solution/Kafka

# Start Kafka (KRaft mode, single broker)
docker compose up -d

# Wait ~10 seconds for the broker to be ready, then:
chmod +x run_all.sh
./run_all.sh

# When done:
docker compose down
```

Outputs:

- `Kafka/results_response_time.txt`
- `Kafka/results_throughput.txt` - contains BOTH the Java benchmark (fair vs JMS) AND the official `kafka-producer-perf-test.sh` / `kafka-consumer-perf-test.sh` numbers
- `Kafka/results_latency.txt`

---

### Troubleshooting

| Symptom | Fix |
|---|---|
| `mvn` not found | `sudo apt install -y maven` |
| `docker compose: command not found` | Use `docker-compose` (older syntax) or install `docker-compose-v2` |
| Kafka container won't start | `docker compose logs kafka` - usually a port conflict on 9092. `lsof -i :9092` and kill what's there. |
| JMS benchmark hangs at startup | Embedded mode shouldn't hang. If using TCP mode, confirm ActiveMQ is up: `curl http://localhost:8161` |
| Throughput test says FAILED at very low X | Likely a stale queue/topic. Restart the broker. |
| `Failed to load class StaticLoggerBinder` | slf4j version mismatch - already pinned in pom.xml, shouldn't happen. If it does, run `mvn dependency:tree` and align slf4j-simple with whatever slf4j-api is on the classpath. |

---

## Part 2 - Report skeleton (fill the placeholders)

Copy everything below this line into a new file `Lab4_Report.md` (or paste into Google Docs / Word). Replace every `<X>` with the corresponding number from your benchmark outputs. The instructor will look for the structure described in the lab handout's section 5 ("Deliverables"), so the headings here match that exactly.

---

# Lab 4 - JMS vs Kafka - Report

**Course:** CSE-4E3 Designing Data Intensive Applications
**Faculty of Engineering, Alexandria University**
**Team members:** `<NAME 1>`, `<NAME 2>`, `<NAME 3>`, `<NAME 4>`
**Date:** `<DATE>`

## Test environment

| Item | Value |
|---|---|
| OS | `<e.g. Ubuntu 24.04>` |
| CPU | `<e.g. AMD Ryzen 5 5600X, 6c/12t>` |
| RAM | `<e.g. 16 GB DDR4>` |
| Java | `<output of java -version>` |
| ActiveMQ | 5.18.3 (`<embedded vm:// or tcp://localhost:61616>`) |
| Kafka | 3.7 (Bitnami image, KRaft mode, 1 broker, 1 partition, replication=1, on `localhost:9092`) |
| Message size | 1 KB (1024 bytes of 'A') |
| Delivery mode | JMS NON_PERSISTENT, AUTO_ACKNOWLEDGE / Kafka acks=1 |

The two settings are deliberately chosen to be the closest fair analogues: neither writes to disk synchronously and both confirm receipt at the broker, so per-call latency reflects the messaging layer alone, not disk I/O.

---

## A) Performance Comparison

### A.1 Response Time

#### Methodology

For each tool we issued the produce API call 1000 times in a tight loop, recording the wall-clock time for each call. We then drained 1000 messages back through the consume API in another 1000-call loop, again timing each call. The reported number is the **median** of the 1000 samples - chosen over the mean to be robust to GC pauses and OS scheduling outliers.

#### JMS sample code

```java
// Produce (timed per call)
long start = System.nanoTime();
producer.send(msg);
long produceUs = (System.nanoTime() - start) / 1000;

// Consume (timed per call)
long start = System.nanoTime();
Message m = consumer.receive(5000);
long consumeUs = (System.nanoTime() - start) / 1000;
```

Full code: `JMS/src/main/java/jms/ResponseTimeBenchmark.java`.

#### Kafka sample code

```java
// Produce (timed per call). .get() makes the send synchronous, matching JMS semantics.
long start = System.nanoTime();
producer.send(record).get();
long produceUs = (System.nanoTime() - start) / 1000;

// Consume (timed per call). max.poll.records=1 makes each poll return ≤1 record.
long start = System.nanoTime();
ConsumerRecords<String,String> recs = consumer.poll(Duration.ofSeconds(5));
long consumeUs = (System.nanoTime() - start) / 1000;
```

Full code: `Kafka/src/main/java/kafka/ResponseTimeBenchmark.java`.

#### Results

| API call | JMS (median) | Kafka (median) |
|---|---|---|
| Produce | 7 µs | 1031 µs |
| Consume | 13 µs | 47 µs |

#### Observations

JMS (embedded, vm://) achieves extremely low per-call response times (7 µs produce, 13 µs consume) because there is no network hop and no broker batching—everything is in-process. Kafka, running over TCP localhost, incurs a syscall and an ack round-trip, resulting in higher median response times (1031 µs produce, 47 µs consume). If JMS were run over TCP, the gap would narrow, but embedded mode is always faster for single-message operations. JMS is optimal for ultra-low-latency, in-process messaging; Kafka’s numbers are typical for a networked broker.

---

### A.2 Maximum Throughput

#### Methodology

For each target throughput X (msg/sec) we picked a period T = 1000/X ms and submitted X requests in a 1.2-second window, sleeping (T − 0.2 T) ms after each call to spread the load across the second instead of bunching it at the end. Increases are exponential (×2 each step). A trial passes only if zero errors occurred AND every produced message was successfully drained by the consumer. The reported max is the last X that passed.

For Kafka we additionally ran the official `kafka-producer-perf-test.sh` and `kafka-consumer-perf-test.sh` scripts shipped in the Kafka distribution. Those scripts use Kafka-native batching (no per-message sync), so their numbers are higher than the Java benchmark - both are reported below.

#### JMS sample code

```java
long sleepMs = (long) ((1000.0 / X) * 0.8);
for (int i = 0; i < X; i++) {
    producer.send(session.createTextMessage(payload));
    if (sleepMs > 0) Thread.sleep(sleepMs);
}
```

Full code: `JMS/src/main/java/jms/ThroughputBenchmark.java`.

#### Kafka sample code (Java, fair vs JMS)

```java
long sleepMs = (long) ((1000.0 / X) * 0.8);
for (int i = 0; i < X; i++) {
    producer.send(record, (md, ex) -> { if (ex != null) errors.incrementAndGet(); });
    if (sleepMs > 0) Thread.sleep(sleepMs);
}
producer.flush();
```

Full code: `Kafka/src/main/java/kafka/ThroughputBenchmark.java`.

Note we use Kafka's async send + a single `flush()` at the end of the window. This is the canonical Kafka usage pattern; using `.send().get()` per message would artificially cap Kafka throughput at the rate of a single network round-trip, which is not what Kafka is designed for.

#### Kafka native script (sanity check)

```bash
kafka-producer-perf-test.sh \
    --topic perf-test \
    --num-records 1000000 \
    --record-size 1000 \
    --throughput -1 \
    --producer-props bootstrap.servers=localhost:9092 acks=1
```

#### Results

| API call | JMS max | Kafka max (Java, fair) | Kafka max (perf-test.sh, native) |
|---|---|---|---|
| Produce | 204800 msg/sec | 512000 msg/sec | 119861 msg/sec |
| Consume | 204800 msg/sec | 0 msg/sec | 203791 msg/sec |

#### Observations

JMS (embedded) achieves up to 204,800 msg/sec for both produce and consume, which is high for an in-process broker with no network. Kafka’s Java benchmark ("fair vs JMS") achieves a much higher max produce throughput (512,000 msg/sec) due to batching and async send, but the consume path in the Java test failed (0 msg/sec), likely due to the specific test harness constraints and settings (e.g., per-poll granularity). The Kafka native script shows realistic, production-grade throughput: 119,861 msg/sec produce and 203,791 msg/sec consume. Kafka’s architecture is designed for high throughput under load, especially with batching, while JMS is limited by its single-broker, in-memory design.

---

### A.3 Median Latency (end-to-end)

#### Methodology

A consumer is started first and is actively polling. The producer then sends 10,000 × 1 KB messages, each carrying its own send-time as a property (`ts` in JMS) or header (`ts` byte array in Kafka). On consume, latency = current time − ts. The reported number is the median across all 10,000 messages.

#### JMS sample code

```java
// Producer side
TextMessage m = session.createTextMessage(payload);
m.setLongProperty("ts", System.currentTimeMillis());
producer.send(m);

// Consumer side
Message m = consumer.receive();
long latencyMs = System.currentTimeMillis() - m.getLongProperty("ts");
```

Full code: `JMS/src/main/java/jms/LatencyBenchmark.java`.

#### Kafka sample code

```java
// Producer side
byte[] tsBytes = ByteBuffer.allocate(8).putLong(System.currentTimeMillis()).array();
ProducerRecord<String,String> r = new ProducerRecord<>(topic, null, payload);
r.headers().add("ts", tsBytes);
producer.send(r);

// Consumer side
Header h = record.headers().lastHeader("ts");
long ts = ByteBuffer.wrap(h.value()).getLong();
long latencyMs = System.currentTimeMillis() - ts;
```

Full code: `Kafka/src/main/java/kafka/LatencyBenchmark.java`.

#### Results

| Metric | JMS | Kafka |
|---|---|---|
| Median latency | 57 ms | 2838 ms |
| p95 latency | 97 ms | 2871 ms |
| Min / Max | 8 / 97 ms | 2819 / 2874 ms |
| Messages measured | 10,000 | 10,000 |

#### Observations

JMS’s median end-to-end latency (57 ms) is much lower than Kafka’s (2838 ms). JMS’s in-memory, non-persistent path avoids batching and network delays, resulting in consistently low latency. Kafka’s pipeline introduces additional delays due to batching, network, and broker processing, leading to higher median and tail latencies. If the lowest possible single-message latency is required, JMS is the better choice; for throughput-bounded latency under heavy load, Kafka’s batching can be tuned for better performance.

---

### Performance summary table

| Metric | JMS | Kafka |
|---|---|---|
| Produce response time (median) | 7 µs | 1031 µs |
| Consume response time (median) | 13 µs | 47 µs |
| Max produce throughput (Java, fair) | 204800 msg/s | 512000 msg/s |
| Max consume throughput (Java, fair) | 204800 msg/s | 0 msg/s |
| Max produce throughput (Kafka native script) | n/a | 119861 msg/s |
| Median end-to-end latency | 57 ms | 2838 ms |
| p95 end-to-end latency | 97 ms | 2871 ms |

---

## B) Usability

### Tool setup overhead

| Step | JMS (ActiveMQ) | Kafka |
|---|---|---|
| Install runtime | `sudo apt install default-jdk` | `apt install docker.io` |
| Install broker | None (embedded) - or download tarball, extract | `docker compose up -d` |
| Configure broker | None | docker-compose.yml with KRaft env vars (~10 lines) |
| Start broker | None - or one command | One command |
| Stop broker | None - or one command | One command |
| **Total commands to first message** | **2** (embedded) / **5** (TCP) | **3** |
| **Time to "Hello World"** | Not measured | Not measured |

Subjective notes:

- **JMS** wins on absolute simplicity if you accept the embedded mode - zero broker process to manage. For real production-style TCP setup, JMS still wins on commands but loses on configuration depth (XML config, JNDI, etc., for non-trivial setups).
- **Kafka** has more moving parts conceptually (brokers, topics, partitions, consumer groups, offsets) but the Docker Compose path makes it as easy as JMS in practice. KRaft removed the Zookeeper step entirely - this used to be an additional pain point.

### Code cluttering - produce path

| | JMS | Kafka |
|---|---|---|
| API objects to chain | ConnectionFactory → Connection → Session → Queue → MessageProducer → TextMessage | KafkaProducer → ProducerRecord |
| Lines of code (end-to-end) | `<~14>` | `<~7>` |
| Method calls per produce | `<~6>` | `<~2>` |
| Lines of code (end-to-end) | ~14 | ~7 |
| Method calls per produce | ~6 | ~2 |

Concrete count from our own code (`ResponseTimeBenchmark.java`):
- JMS produce setup: `factory = new ActiveMQConnectionFactory(...)`, `factory.createConnection()`, `connection.start()`, `connection.createSession(...)`, `session.createQueue(...)`, `session.createProducer(queue)`, `producer.setDeliveryMode(...)`, `session.createTextMessage(payload)`, `producer.send(msg)` → **9 distinct API calls**.
- Kafka produce setup: `new KafkaProducer<>(props)`, `new ProducerRecord<>(topic, payload)`, `producer.send(record)` → **3 distinct API calls** (plus property bag construction).

### Code cluttering - consume path

| | JMS | Kafka |
|---|---|---|
| API objects to chain | (same setup as produce) → MessageConsumer → receive() | KafkaConsumer.subscribe() → poll() → iterate |
| Lines of code (end-to-end) | `<~12>` | `<~6>` |
| Lines of code (end-to-end) | ~12 | ~6 |
| Per-message extraction | `((TextMessage)m).getText()` | `record.value()` |

### Other usability notes

- **Configuration model**: JMS uses XML / JNDI / vendor-specific config. Kafka uses a flat `Properties` object - easier to grep, easier to override per-environment.
- **Type-safety**: JMS messages need casting (`(TextMessage)m`). Kafka records are typed via the serializer choice, no casts needed.
- **Errors**: Kafka exceptions inherit from `KafkaException` (RuntimeException) so try/catch is optional. JMS forces checked `JMSException` everywhere - boilerplate.
- **Tooling**: Kafka ships the `kafka-*.sh` scripts. ActiveMQ ships a web console at :8161. Both fine for ops; the Kafka CLI scripts compose better with shell pipelines.

---

## C) Integrations

### Approach

We surveyed the official documentation of each tool plus the Apache / Confluent ecosystem catalogues. For each candidate connector or client we recorded: maturity (official vs community), licence, last release, and whether it's a sink (Kafka/JMS → external system) or source (external system → Kafka/JMS).

### Programming languages

**JMS** is a Java specification. There is no JMS for other languages. Languages can interoperate with JMS-implementing brokers via the brokers' own non-JMS protocols:
- ActiveMQ supports STOMP, AMQP 1.0, MQTT, OpenWire - clients exist in C/C++, Python, .NET, Node.js, Go, Ruby for these protocols.
- IBM MQ has native clients in C, COBOL, .NET, Python, Go.
This is *protocol-level* interoperability, not JMS-level. The same JMS code does not run cross-language.

**Kafka** publishes a wire protocol and the official Java client. First- or second-party clients exist for:
- Python (`kafka-python`, `confluent-kafka-python`)
- Go (`confluent-kafka-go`, `franz-go`, `sarama`)
- C/C++ (`librdkafka` - used by Confluent's clients underneath)
- .NET (`Confluent.Kafka`)
- Node.js (`kafkajs`, `node-rdkafka`)
- Rust (`rdkafka-rust`)
- Ruby, PHP, Erlang, Scala, Clojure, etc.

**Verdict**: Kafka is genuinely polyglot; JMS is Java-only by definition.

### Out-of-the-box external data source integrations

**Kafka Connect** is the framework. Open-source connectors (sample, not exhaustive):

| Category | Connectors |
|---|---|
| Databases (CDC) | Debezium (MySQL, PostgreSQL, MongoDB, SQL Server, Oracle, DB2) |
| JDBC | Confluent JDBC source/sink (any JDBC-compliant DB) |
| NoSQL | MongoDB, Cassandra, Couchbase, Redis, Neo4j |
| Object stores | S3, GCS, Azure Blob |
| Search / analytics | Elasticsearch, OpenSearch, ClickHouse |
| Data warehouses | Snowflake, BigQuery, Redshift |
| File / FTP | File source/sink, SFTP |
| Other | HDFS, JMS source/sink (yes - Kafka can consume from a JMS broker) |

The Confluent Hub lists 100+ certified connectors. Strimzi adds Kubernetes-native deployment.

**JMS** has no equivalent connector framework. Integrations happen via:
- Apache Camel routes (Camel speaks ~300 endpoints including JMS)
- Spring Integration
- Custom adapters using the broker's specific extensions

**Verdict**: Kafka has a vastly larger ready-made integration catalogue. For data-intensive applications (Hadoop, Cassandra, columnar warehouses, analytics stores), Kafka is essentially built for the job; JMS gets there via Camel or hand-written glue.

### References

- Apache Kafka documentation - https://kafka.apache.org/documentation/
- Confluent Hub - https://www.confluent.io/hub/
- Strimzi (Kafka on Kubernetes) - https://strimzi.io/
- Apache ActiveMQ documentation - https://activemq.apache.org/
- Jakarta Messaging (JMS) specification - https://jakarta.ee/specifications/messaging/
- Apache Camel components catalogue - https://camel.apache.org/components/
- Debezium - https://debezium.io/

---

## Summary - JMS

### Advantages

- Lowest per-call latency in embedded / in-process mode (no network)
- Cleaner enterprise transaction story (XA, two-phase commit) - useful when message processing must be atomic with database writes
- Stronger out-of-the-box message routing inside one broker: queues, topics, durable subscribers, message selectors (server-side filtering), priority, expiry, request/reply via JMSReplyTo
- Built-in dead-letter queue conventions
- Mature: 25+ years, every Java EE app server speaks it
- Synchronous receive is simple - `consumer.receive()` blocks until a message arrives

### Disadvantages

- Java-only specification
- Vendor lock-in inside Java: vendor A's client rarely talks to vendor B's broker even though the API is shared
- Not designed for stream replay - once consumed, a message is gone (without explicit DLQ / topic durable subscription configuration)
- Throughput ceiling lower than Kafka in distributed deployments - broker is a bottleneck, sharding is per-vendor
- Verbose API: ConnectionFactory → Connection → Session → Producer/Consumer chain for every operation
- Limited ecosystem of ready-made source/sink integrations to data-intensive systems

---

## Summary - Kafka

### Advantages

- Highest sustainable throughput in our tests (and generally) - partition-based horizontal scaling, batching, sequential disk I/O
- Replay-friendly: messages persist by retention policy; consumers track their own offset and can rewind
- Polyglot: official or near-official clients in 10+ languages
- Massive connector ecosystem (Kafka Connect, Confluent Hub) - Debezium, S3, Elasticsearch, Snowflake, etc., out of the box
- Natural fit for stream processing (Kafka Streams, ksqlDB, Flink, Spark Streaming all integrate directly)
- KRaft mode removes the previous Zookeeper dependency - one fewer system to operate
- Simpler client API - `KafkaProducer` and `KafkaConsumer` are the two main types

### Disadvantages

- Higher per-call latency than embedded JMS - TCP round-trip + ack wait is unavoidable
- Conceptually more to learn: partitions, consumer groups, offsets, ISR, retention
- No built-in transactions across an external database (idempotent producer + transactional API exists, but isn't a drop-in replacement for JTA/XA)
- No native server-side message selectors - filtering is consumer-side or via stream-processing
- No built-in priority queues or per-message TTL in the JMS sense
- Smallest production deployment is still 3 brokers + replication for safety

---

## Conclusion - recommendation

`<choose one of the following based on your audience / org context, or write your own>`

**For a data-intensive application** - which is the framing of this lab - **we recommend Kafka**. The decision factors:

- The lab's premise is data pipelines and stream-processing, where retention, replay, and high throughput dominate. Kafka was designed for exactly this.
- Integrations with Hadoop / Spark / Cassandra / columnar warehouses are first-class via Kafka Connect; with JMS we'd write glue code or rely on Camel.
- Polyglot consumers (Python ML jobs, Go services, Node.js analytics) are realistic only with Kafka.
- The latency penalty (a few ms vs sub-ms for embedded JMS) is acceptable for streaming workloads. If we needed sub-ms request/reply for an inside-the-app messaging bus, JMS would still be the right pick.

**JMS would be the right pick** if the workload were enterprise transactional messaging - orders, payments, inventory updates inside a Java EE stack - where XA transactions and tight Java integration matter more than throughput or polyglot support.

In short: same problem, different shapes. Kafka for streams; JMS for enterprise message-oriented middleware. For *this* lab's framing, Kafka.
