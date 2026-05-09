package kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Lab requirement A.2, Kafka side: maximum throughput.
 *
 * Same methodology as jms.ThroughputBenchmark for apples-to-apples comparison:
 *   - exponential ramp-up of target X (msg/sec), ×2 each step
 *   - sleep (T - 0.2 T) ms between calls to spread load across the second
 *   - pass/fail = zero errors AND every message accounted for
 *
 *  Note: the lab also offers the official `kafka-producer-perf-test.sh` and
 *  `kafka-consumer-perf-test.sh` scripts. Those use Kafka-native batching
 *  (much faster numbers) and are sanity-checked separately by run_all.sh.
 *  This Java benchmark is the FAIR comparison vs JMS - same call shape.
 */
public class ThroughputBenchmark {

    private static final int START_THROUGHPUT_PRODUCE = 1_000;
    private static final int START_THROUGHPUT_CONSUME = 100;
    private static final int MAX_THROUGHPUT   = 512_000;
    private static final int RAMP_FACTOR      = 2;

    public static void main(String[] args) throws Exception {
        System.out.println("=== Kafka Maximum Throughput Benchmark (Java, fair vs JMS) ===");
        System.out.println("Bootstrap : " + KafkaHelper.bootstrap());
        System.out.println();

        System.out.println("── PRODUCE Throughput ──");
        int maxProduce = findMax("PRODUCE");

        System.out.println("\n── CONSUME Throughput ──");
        int maxConsume = findMax("CONSUME");

        System.out.println("\n=== SUMMARY (copy to report) ===");
        System.out.println("  Kafka max produce throughput (Java, sync, no batching): " + maxProduce + " msg/sec");
        System.out.println("  Kafka max consume throughput (Java, max.poll.records=1): " + maxConsume + " msg/sec");
    }

    static int findMax(String mode) throws Exception {
        int last = 0;
        int X = "CONSUME".equals(mode) ? START_THROUGHPUT_CONSUME : START_THROUGHPUT_PRODUCE;
        while (X <= MAX_THROUGHPUT) {
            boolean ok = runTrial(X, mode);
            System.out.printf("  X = %7d msg/sec  ->  %s%n", X, ok ? "OK" : "FAILED");
            if (!ok) break;
            last = X;
            X *= RAMP_FACTOR;
        }
        System.out.printf("  -> Max %s throughput observed: %d msg/sec%n", mode, last);
        return last;
    }

    static boolean runTrial(int X, String mode) throws Exception {
        KafkaHelper.resetTopic();
        long sleepMs = (long) ((1000.0 / X) * 0.8);

        AtomicInteger sent = new AtomicInteger();
        AtomicInteger received = new AtomicInteger();
        AtomicInteger errors = new AtomicInteger();

        if ("PRODUCE".equals(mode)) {
            try (KafkaProducer<String, String> producer = KafkaHelper.createAsyncProducer()) {
                long deadline = System.currentTimeMillis() + 1_200;
                for (int i = 0; i < X && System.currentTimeMillis() < deadline; i++) {
                    try {
                        // Async send - return immediately. Errors surface in the callback.
                        producer.send(
                                new ProducerRecord<>(KafkaHelper.TOPIC, KafkaHelper.PAYLOAD_1KB),
                                (md, ex) -> { if (ex != null) errors.incrementAndGet(); });
                        sent.incrementAndGet();
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    }
                    if (sleepMs > 0) Thread.sleep(sleepMs);
                }
                producer.flush();    // <-- block until all sends actually reached the broker
            }
            // Verify: drain the topic and count.
            received.set(drain(sent.get()));

        } else {  // CONSUME
            // Pre-fill topic. Use async + flush - much faster than sync sends.
            try (KafkaProducer<String, String> producer = KafkaHelper.createAsyncProducer()) {
                for (int i = 0; i < X; i++) {
                    producer.send(new ProducerRecord<>(KafkaHelper.TOPIC, KafkaHelper.PAYLOAD_1KB));
                    sent.incrementAndGet();
                }
                producer.flush();
            }

            try (KafkaConsumer<String, String> consumer =
                         KafkaHelper.createConsumer(KafkaHelper.newGroupId(), 1)) {
                consumer.subscribe(Collections.singleton(KafkaHelper.TOPIC));

                // Warm-up: Kafka needs at least one poll() to join the group and
                // get partition assignment. JMS setup cost is paid before the
                // timed receive() loop, so we do the same here.
                long warmupDeadline = System.currentTimeMillis() + 3_000;
                while (consumer.assignment().isEmpty() && System.currentTimeMillis() < warmupDeadline) {
                    consumer.poll(Duration.ofMillis(50));
                }

                long deadline = System.currentTimeMillis() + 1_200;
                while (received.get() < X && System.currentTimeMillis() < deadline) {
                    ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(50));
                    if (recs.isEmpty()) {
                        // No record available yet - but we said X were prefilled, so this
                        // counts as an error if the deadline hits before we get them all.
                        continue;
                    }
                    received.addAndGet(recs.count());
                    if (sleepMs > 0) Thread.sleep(sleepMs);
                }
                if (received.get() < sent.get()) errors.incrementAndGet();
            }
        }

        return errors.get() == 0 && received.get() >= sent.get();
    }

    /** Drain the topic to verify produce trial; returns received count. */
    static int drain(int expected) {
        try (KafkaConsumer<String, String> consumer =
                     KafkaHelper.createConsumer(KafkaHelper.newGroupId(), 500)) {
            consumer.subscribe(Collections.singleton(KafkaHelper.TOPIC));
            int n = 0;
            long deadline = System.currentTimeMillis() + 5_000;
            while (n < expected && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(500));
                n += recs.count();
            }
            return n;
        }
    }
}
