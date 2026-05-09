# EXPLANATION - JMS vs Kafka

This file is for *you*, before the discussion. It's structured to read top-to-bottom: starting from the highest-level concepts (what is a message queue?), descending into how JMS and Kafka actually work under the hood, then a concrete walk-through of the lab's benchmark methodology, and finally a TA Q&A section with the questions instructors typically ask.

---

## Part 1 - Conceptual layer

### Why message queues exist at all

Two services that need to talk can either:

1. Call each other directly (HTTP, gRPC, RPC). Producer waits for consumer. If consumer is down or slow, producer is stuck. If consumer is fast but bursty, producer is fine but consumer drops requests.

2. Send messages through an intermediary (the *broker*). Producer hands off the message and continues. Consumer reads when it's ready. The broker absorbs bursts, holds messages while the consumer is down, and decouples both ends in time.

Option 2 buys you three properties at once:

- **Asynchrony** - producer doesn't wait.
- **Decoupling** - producer doesn't need to know who the consumers are or how many; consumers don't need to know which producer sent what.
- **Buffering** - bursts above the consumer's processing rate don't get dropped; they wait in the queue.

The cost: you now operate a broker, and you have to reason about delivery guarantees (does every message arrive? can a message arrive twice? in what order?).

### The two big delivery models

**Point-to-point (queue).** One message, one consumer. If three consumers are subscribed to the same queue, the broker delivers each message to *one* of them, balancing load. Used for work distribution: "process this order", "resize this image".

**Publish-subscribe (topic).** One message, every subscriber gets a copy. If three consumers subscribe, all three see every message. Used for fan-out: "user signed up - notify analytics, email service, and recommendations".

JMS supports both models with different APIs (`Queue` vs `Topic`). Kafka collapses the distinction: every Kafka topic is a partitioned log; "queue-like" behaviour comes from putting consumers in the same consumer group, "topic-like" behaviour comes from putting them in different groups.

### Delivery guarantees

Three levels, from weakest to strongest:

| Guarantee | What it means | Cost |
|---|---|---|
| At-most-once | Message may be lost; never duplicated. | Cheapest. Fire and forget. |
| At-least-once | Message will be delivered; may be delivered more than once. | Default in most systems. Consumers must be idempotent. |
| Exactly-once | Message delivered exactly one time. | Expensive. Needs transactions, deduplication, or both. |

Both JMS and Kafka can be configured for any of the three, with different mechanisms.

### Push vs pull

- **Push** - broker sends to consumer as soon as a message arrives. Low latency. If consumer is slow, broker buffers (or drops, depending on config).
- **Pull** - consumer asks the broker "anything new?". Consumer controls its own rate. Slightly higher latency unless polling is aggressive.

JMS supports both: `consumer.receive()` is pull (sync); `MessageListener` is push (async callback). Kafka is fundamentally pull - `consumer.poll()` is the only way data flows out.

### The broker - what's actually in the middle

A broker is a process that receives messages, stores them (briefly or for a long time), and hands them out to consumers. The interesting questions are:

- **Persistence**: does the broker keep messages on disk so they survive a restart? JMS has `PERSISTENT` vs `NON_PERSISTENT`. Kafka always writes to disk but trades disk for sequential append - which is much faster than random writes.
- **Replication**: does the broker have copies of each message on multiple nodes? Required for HA. JMS providers do this differently per vendor; Kafka does it via the in-sync replica (ISR) protocol.
- **State of consumers**: who has read what? JMS brokers track this themselves (acknowledgements). Kafka doesn't - the consumer remembers its own offset and tells the broker when to commit it. This is the single biggest architectural difference.

Why does this matter? Because when the broker tracks per-consumer state, the broker becomes the bottleneck and the source of truth, but consumers can be dumb. When the consumer tracks its own state, the broker can be a simple append-only log shared by anyone, but consumers must be smart enough to remember their position. Kafka picked the second. That choice is why Kafka scales so much better - but it's also why "let me skip the messages I already processed" is a one-line config in JMS and a multi-step procedure in Kafka.

---

## Part 2 - JMS technical layer

### What JMS *is*

JMS is **an API specification**, not a protocol and not a product. It's part of the Jakarta EE standard (formerly Java EE). It's a set of Java interfaces - `Connection`, `Session`, `MessageProducer`, `MessageConsumer`, `Message` and friends - that any compliant broker provides an implementation for.

That has two consequences:

1. **It's Java-only.** No Python JMS, no Go JMS - the spec is in `javax.jms` (or `jakarta.jms` post-2019).
2. **Implementations are vendor-specific.** ActiveMQ, IBM MQ, Solace, Tibco EMS, RabbitMQ (with the JMS plugin), Artemis - they all expose the same `javax.jms.*` interfaces but their wire protocols and broker internals are completely different. You can't point an IBM MQ JMS client at an ActiveMQ broker.

### The JMS object hierarchy

```
ConnectionFactory      ← created from JNDI or a vendor-specific factory class
       │
       └─ Connection   ← long-lived TCP connection to the broker
              │
              └─ Session   ← thread-bound message context (single-threaded by spec)
                     │
                     ├─ Queue / Topic   ← destination handle
                     │
                     ├─ MessageProducer ← sends to a destination
                     │
                     └─ MessageConsumer ← receives from a destination
```

This nesting is why JMS produce/consume code is verbose: you can't send a message without setting up the whole chain. Each layer carries configuration (transaction mode, ack mode, persistence, priority, expiry).

### Acknowledgement modes

When a session is created, you pick how the consumer tells the broker "I've got this message, you can delete it":

- `AUTO_ACKNOWLEDGE` - automatic on successful return from `receive()`. Easiest. Used in this lab.
- `CLIENT_ACKNOWLEDGE` - consumer calls `message.acknowledge()` explicitly. Lets you batch acks.
- `DUPS_OK_ACKNOWLEDGE` - lazy ack. Better throughput, but the broker may redeliver after a crash.
- `SESSION_TRANSACTED` - messages are acknowledged as part of a JMS transaction (`session.commit()` / `session.rollback()`).

### Delivery mode

Per-message: `PERSISTENT` (broker writes to disk before ack) or `NON_PERSISTENT` (broker may keep in memory only). The lab uses NON_PERSISTENT so disk I/O isn't a hidden variable.

### Message selectors

A JMS-distinctive feature: consumers can register an SQL-like filter at subscribe time.

```java
session.createConsumer(queue, "OrderType = 'PREMIUM' AND Total > 100");
```

The broker evaluates the filter and only delivers matching messages. Kafka has no equivalent - filtering happens consumer-side or via a stream processor.

### Why ActiveMQ specifically

For this lab we use ActiveMQ Classic 5.18.3 because:

- It's the canonical open-source JMS broker.
- It runs trivially in **embedded (in-process) mode** via the `vm://localhost` URL - no separate process, no install. The broker code runs inside the same JVM as the client.
- It also speaks STOMP, AMQP, OpenWire, MQTT - useful for the integrations section even if we don't use them in benchmarks.

### What does an in-memory `vm://` broker actually skip?

When you connect to `vm://localhost`, ActiveMQ's broker classes are instantiated inside your JVM. There is no socket, no serialization, no syscall - `producer.send()` calls into broker code directly. That's why the response-time numbers can come out in single-digit microseconds. They are *valid* numbers but they describe the cost of "JMS, theoretical minimum, no broker process". When comparing against Kafka, which always uses TCP, this gap will look enormous. We document this honestly in the report and offer a TCP-broker mode for a fair comparison.

---

## Part 3 - Kafka technical layer

### What Kafka *is*

Kafka is **a distributed append-only log**. Saying it's a "message queue" is technically true but misses the point - Kafka is closer to a database than to a traditional broker. The core abstraction is the *partition*: an ordered, immutable sequence of records that you can only append to, identified by an integer offset starting at 0.

```
Partition 0: [r0][r1][r2][r3][r4][r5]...
                                     ^
                                  log end offset
```

A *topic* is a logical name that maps to one or more partitions. A topic with 4 partitions is 4 independent logs that share a name.

### Producers

A `KafkaProducer` is a long-lived client object. Its job, given a `ProducerRecord(topic, key, value)`:

1. Pick a partition. Default: hash the key. No key → round-robin (with sticky-batching since 2.4).
2. Serialise key and value (using your configured serialisers).
3. Append the record to a per-partition in-memory batch.
4. When a batch is "full enough" (size hits `batch.size`) or "old enough" (age hits `linger.ms`), flush it to the broker as one network request.

The two knobs that matter for performance:

- `linger.ms` - how long the producer is willing to wait to fill a batch. 0 = send immediately; 5 = wait up to 5 ms.
- `batch.size` - max bytes per batch.

In our **response-time** benchmark we use `linger=0, batch=0` so each `send()` is its own request - that's the only honest way to measure per-call latency. In our **throughput** and **latency** benchmarks we use the defaults so the producer is allowed to batch - that's the only honest way to measure how fast Kafka actually goes.

### `acks` setting

How many brokers must confirm a write before `send()` is considered successful:

- `acks=0` - fire and forget. Producer doesn't wait. Closest analog to "fire packet at UDP". Fastest, least durable.
- `acks=1` - leader broker confirms. Followers may not have the data yet. Default in older Kafka. Used in this lab.
- `acks=all` - all in-sync replicas confirm. Strongest durability. Slower.

### Consumers and consumer groups

A `KafkaConsumer` reads records. Each consumer belongs to a **consumer group** (identified by `group.id`). Within a group, partitions are split among consumers - Kafka's group coordinator assigns partitions on join and rebalances on member changes.

- Same group, multiple consumers → **work distribution** (queue semantics). Each record goes to exactly one consumer in the group.
- Different groups → **fan-out** (topic semantics). Each group sees every record independently.

The consumer remembers an offset per partition. Two storage options for that offset:

- Auto-commit (`enable.auto.commit=true`) - Kafka commits periodically.
- Manual commit (`consumer.commitSync()` / `commitAsync()`) - you control when.

We use manual commit + earliest offset reset in the benchmarks so each run starts fresh.

### `poll()` semantics

`consumer.poll(Duration.ofSeconds(5))` does a lot in one call:

1. Sends fetch requests to the brokers holding our assigned partitions.
2. Waits up to `fetch.max.wait.ms` (default 500 ms) for at least `fetch.min.bytes` (default 1 byte) to come back.
3. Hands records to the consumer in batches. The number returned is bounded by `max.poll.records` (default 500).
4. Also drives heartbeats and rebalance protocol - must be called regularly.

For the response-time benchmark we set `max.poll.records=1` so each poll returns at most one record, mirroring JMS's per-message `receive()`. For throughput we let the default kick in.

### Replication and the in-sync replica set (ISR)

Each partition has one leader and zero or more followers (replicas). Followers continually fetch from the leader. A follower that is "caught up enough" (within `replica.lag.time.max.ms`) is considered in-sync. The set of currently in-sync replicas is the ISR.

When `acks=all`, the leader waits for all ISR members to confirm before acking the producer. This is how Kafka guarantees durability without losing throughput to replication round-trips.

In our single-broker lab setup, ISR = {leader}, replication-factor = 1 - so durability guarantees are minimal. Acceptable for a benchmark; not acceptable for production.

### KRaft mode (vs Zookeeper)

Older Kafka used Apache Zookeeper to elect controllers and store cluster metadata. Since Kafka 3.3 (2022) the **KRaft** mode is GA: Kafka uses an internal Raft consensus protocol so brokers handle their own metadata. Operationally this means: one fewer system to install, fewer ports, simpler deployment.

Our `docker-compose.yml` runs KRaft mode (`KAFKA_CFG_PROCESS_ROLES=controller,broker`). No Zookeeper container needed.

### Why is Kafka fast?

A few non-obvious tricks:

- **Sequential disk I/O**. Append-only writes to a log file are roughly as fast as memory access on modern SSDs and HDDs alike - no seek time. Random writes (which a typical broker does when tracking per-message state) are 100× slower.
- **Page cache**. Kafka writes to disk via the OS page cache. Recent messages are read back from cache, not disk. Kafka deliberately doesn't maintain its own message cache - let the OS do it.
- **Zero-copy** (`sendfile()`). When sending bytes from disk to a TCP socket, Kafka uses the `sendfile()` syscall to skip the userspace round-trip - bytes go disk → kernel → socket without ever entering the JVM heap.
- **Batching at every layer**. Producer batches before send. Broker writes batched. Consumer fetches batched. Compression is per-batch. The cost of "one record" amortises across thousands.
- **No per-consumer state in the broker**. Just an offset, stored in a special compacted topic (`__consumer_offsets`). Adding a million consumers doesn't make the broker work harder.

---

## Part 4 - Side-by-side architecture

| Aspect | JMS (e.g. ActiveMQ) | Kafka |
|---|---|---|
| Surface area | Java API spec | Wire protocol + clients in 10+ languages |
| Storage model | Per-message records, often in a database-like store | Append-only partitioned log on disk |
| Consumer state | Tracked by broker (per-message ack) | Tracked by consumer (offset commits) |
| Delivery | Push (listener) or pull (receive) | Pull only (poll) |
| Replay | Generally one-shot - once acked, gone | Native - rewind offset to any retained position |
| Filter on broker | Yes (message selectors, SQL-92 subset) | No |
| Transactions | XA / two-phase commit supported | Idempotent producer + transactional API (Kafka-internal only) |
| Ordering | Per-queue (FIFO with caveats) | Per-partition (strict) |
| Scaling unit | Vendor-specific (clusters, networks of brokers) | Partitions |
| Typical latency | Sub-millisecond achievable in-memory | Few milliseconds, network-bound |
| Typical throughput | Tens of thousands of msg/s per broker | Hundreds of thousands to millions of msg/s per broker |
| Built-in retention | None - message disappears on ack | Time- or size-based, configurable per topic |
| Ecosystem of connectors | Modest, mostly via Camel / Spring | Huge - Kafka Connect, Debezium, Confluent Hub |

---

## Part 5 - Walking through the lab benchmarks

### Why median, not mean?

Means are pulled around by outliers. A single 200 ms GC pause among 999 sub-millisecond runs would push the mean from ~1 ms to ~1.2 ms. The median doesn't notice it. Since we care about *typical* per-call cost, median is the right choice.

We additionally report min, max, and p95 in the latency benchmark to show the tail.

### Why 1 KB messages?

Two reasons. First, the lab fixes it (so all teams produce comparable numbers). Second, 1 KB is small enough that we measure messaging overhead, not network bandwidth - at 1 KB, even a saturated 1 Gbps localhost link tops out around 125k msg/s on raw bytes alone, far above what the messaging layer adds.

Smaller messages (e.g. 100 B) would be dominated by per-call fixed overhead. Larger messages (1 MB) would be dominated by serialisation and network - at that point you're benchmarking your CPU, not Kafka.

### Why 1000 runs for response time?

Statistical stability for the median. The median of 30 samples can swing ±10% run-to-run. The median of 1000 swings by maybe 1–2%. Diminishing returns kick in around 1000–10000 - not worth going higher.

### Why exponential ramp-up for throughput?

We don't know in advance whether the max is 5k or 500k msg/s. Linear search (1k, 2k, 3k, ...) could take 500 trials. Exponential search (1k, 2k, 4k, 8k, ..., 256k) takes 9 trials to bracket either end of that range. The lab's instructions explicitly call this out.

A more refined approach would bisect after the first failure to find a tighter bound. We chose simplicity - the doubling is enough resolution for this scale.

### Why T − 0.2T sleep?

If you call `send()` in a tight loop, the loop runs as fast as the API allows, finishing the X messages in <1 second and saying "yep, achieved X msg/s" - but really you found "how many messages can fit in the API as fast as possible", which is the same as just running unthrottled. That's not what "throughput X" means.

Conversely, a perfectly even spacing of `T = 1000/X` ms between calls would account for zero per-call cost. In practice the call itself takes some time, so each call really happens at `T + call_cost`, and the total exceeds 1 second.

The 0.8 T factor (sleep for `T - 0.2T = 0.8 T`) accounts approximately for that per-call cost. It's a heuristic - the lab itself says it "may fail to compensate, but better than nothing".

At very high X, T rounds down to 0 and `Thread.sleep(0)` becomes a no-op - that's the natural ceiling where throttling stops mattering.

### Why timestamp embedding for latency?

End-to-end latency = (time consumed) − (time produced). If we used the wall clock at both ends without embedding the timestamp in the message, we'd need the producer and consumer clocks to be synchronised - and they're never perfectly synchronised even on the same machine. By embedding the produce-side timestamp inside the message itself and reading it at consume-time, we use one clock for both reads. The difference is a true elapsed time.

(The producer and consumer threads in our benchmarks are in the same JVM, so technically one clock - but the technique generalises.)

### Why is JMS embedded so much faster than Kafka in our numbers?

Because the embedded JMS broker has no IPC. `producer.send()` calls broker code in the same heap, with no syscall, no serialisation, no socket. Kafka *always* uses TCP, even on localhost - there's a syscall and an ack round-trip on every single call.

This is a true result, not a measurement artefact. But it's only meaningful if you actually deploy your broker in-process. Most production JMS deployments use a separate broker process over TCP, and at that point the gap collapses to the network round-trip, plus a few protocol differences.

The TCP-mode JMS configuration we provide via `BROKER_URL=tcp://localhost:61616` gives the apples-to-apples comparison and is the more representative number.

---

## Part 6 - Likely TA questions, with answers

> **"Why did you pick `acks=1` for Kafka?"**

Because the JMS benchmarks use `NON_PERSISTENT` + `AUTO_ACKNOWLEDGE`, which means the broker confirms receipt to the producer but doesn't write to disk and doesn't replicate. The closest Kafka analogue is `acks=1`: leader confirms, no fsync, no replicas. We deliberately matched the durability levels so the comparison reflects the messaging layer alone, not different disk policies.

> **"Why didn't you use `producer.send().get()` in the throughput benchmark?"**

Because Kafka's producer is designed around batching. Calling `.get()` on every send forces one network round-trip per message and disables internal batching - that doesn't measure Kafka's max throughput, it measures the worst-case anti-pattern. For the throughput question we used async `send()` + `flush()` at the end of the test window. For the response-time question, where each call is the unit of measurement, we did call `.get()` - there it's the right thing.

> **"Are your throughput numbers fair if Kafka uses batching and JMS doesn't?"**

This is the right question. JMS's `producer.send()` in NON_PERSISTENT mode also has internal optimisations - async send to broker, buffering on the connection. It's not a per-message sync call. The broker just happens to process individual messages once they arrive. So both sides are doing some kind of batching internally. The "fair" comparison is "whatever the API does when used normally", which is what we measured.

We additionally ran `kafka-producer-perf-test.sh` (the official tool) as a sanity check; it shows what Kafka's max looks like when fully tuned, and it's the industry-standard reference number.

> **"Why is your latency higher for Kafka than JMS?"**

Two reasons. First, the embedded JMS broker has no IPC; Kafka always has a TCP round-trip. Second, the Kafka producer waits up to `linger.ms` (5 ms in our config) to fill a batch before sending. That linger time is added to per-message latency. We could reduce it by setting `linger.ms=0`, at the cost of throughput. The default trade-off favours throughput, which is appropriate for Kafka's typical use case.

> **"Why is your throughput higher for Kafka, given that JMS embedded had lower per-call latency?"**

Because throughput and per-call latency aren't the same axis. Throughput is messages per second under sustained load; per-call latency is time-from-call-to-return. A pipelined system with high latency but high concurrency can have throughput vastly above a single-threaded zero-latency one. Kafka pipelines aggressively (batch in producer, batch on disk, batch on fetch), so even though each individual message takes longer, the throughput is higher. JMS embedded is fast per-call but each call is a synchronous in-process operation - there's no parallelism to harvest.

> **"What would change if you used `acks=all` and PERSISTENT mode?"**

Both numbers would drop. JMS PERSISTENT writes to disk on every produce, so the broker fsync becomes the bottleneck - likely 10–100× slower for produce response time. Kafka `acks=all` waits for all ISR replicas - in our single-broker setup ISR is just the leader so the difference is small, but in a 3-broker production setup it'd add a network round-trip to two replicas.

> **"Why does Kafka require Zookeeper... wait, does it?"**

It used to. Kafka < 3.3 used Apache Zookeeper to elect the controller and store metadata. Since 3.3 GA, Kafka has KRaft mode where brokers run their own consensus protocol. We use KRaft in our `docker-compose.yml`. As of Kafka 4.0 (2024), Zookeeper mode is removed entirely.

> **"What happens in Kafka if a consumer dies mid-message?"**

Depends on offset commit timing. If the consumer had auto-commit on with the default 5-second interval, anything processed since the last commit will be redelivered to whichever group member takes over the partition - this is at-least-once delivery. Application code must be idempotent. With manual commit you control the boundary precisely.

> **"What's a partition for?"**

Two things. First, scaling: each partition is independent, so multiple brokers can serve the same topic in parallel. Second, ordering: Kafka guarantees order within a partition but not across partitions. If you need strict order over a subset of records (e.g. all events for one user ID), you key by user ID - same key always lands in the same partition, hence in order.

> **"If JMS is older and Java-only, why is it still used?"**

Because lots of enterprise Java systems already use it. Strong transactional integration with JTA / XA matters for banking, insurance, telecoms - domains where atomic "send message AND update database" semantics are non-negotiable. Kafka is catching up here (transactional API, Kafka-Connect-with-2PC) but isn't a drop-in replacement for those workloads. JMS also remains the right tool when the use case is truly request/reply or queue-based work distribution rather than streaming.

> **"Recommendation?"**

For data-intensive applications - the framing of this lab - Kafka. Streaming, replay, and integrations are first-class. For enterprise transactional workloads inside a Java EE stack, JMS. The two are not actually direct competitors at the architecture level; they solve overlapping but distinct problems.

> **"What did you actually run, and what did you skip?"**

We ran all three required performance metrics on both tools (response time, throughput, latency), exactly as the lab specified. For Kafka we additionally ran the official `kafka-producer-perf-test.sh` and `kafka-consumer-perf-test.sh` because the lab handout suggested them. We did NOT use JMeter for JMS - wrote our own Java throughput benchmark instead so the methodology is identical to the Kafka Java benchmark, line-for-line. JMeter would have introduced a separate harness with its own overhead, which we didn't want polluting the comparison.

> **"What are the limitations of your benchmarks?"**

Four big ones, in roughly decreasing order of impact:

1. **Single machine, single broker.** Production deployments are clusters. Inter-broker replication, network partitions, partition rebalancing - none of that is exercised.
2. **Embedded vs TCP for JMS.** Embedded is a measurement floor, not a real deployment. We provide a TCP mode and recommend the report mention both.
3. **No persistence.** PERSISTENT JMS and `acks=all` Kafka would change the picture significantly.
4. **No client-side concurrency.** Single producer thread, single consumer thread. Real apps would have many of each. Kafka in particular benefits massively from multiple producers / partitions / consumer-group members.

These are honest limitations, not bugs - the lab is bounded in scope. Mentioning them in the report shows methodological awareness.
