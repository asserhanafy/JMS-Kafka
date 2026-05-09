package kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Lab requirement A.3, Kafka side: median end-to-end latency.
 *
 *   1. Spin up a consumer that's already polling the topic.
 *   2. Producer attaches System.currentTimeMillis() as an 8-byte header "ts".
 *   3. On consume, latency = now() - long-from-header("ts").
 *   4. 10K messages. Report median.
 *
 *  Mirrors jms.LatencyBenchmark exactly. The only Kafka-specific bit is
 *  that the timestamp goes in a *record header* rather than a JMS property.
 */
public class LatencyBenchmark {

    private static final int COUNT = 10_000;
    private static final String TS_HEADER = "ts";

    public static void main(String[] args) throws Exception {
        System.out.println("=== Kafka Median Latency Benchmark ===");
        System.out.println("Bootstrap : " + KafkaHelper.bootstrap());
        System.out.println("Messages  : " + COUNT + "\n");

        KafkaHelper.resetTopic();

        List<Long> latencies = new CopyOnWriteArrayList<>();
        CountDownLatch consumerReady = new CountDownLatch(1);
        CountDownLatch consumerDone  = new CountDownLatch(COUNT);

        // ---------------------------- Consumer ------------------------------
        Thread consumerThread = new Thread(() -> {
            try (KafkaConsumer<String, String> consumer =
                         KafkaHelper.createConsumer(KafkaHelper.newGroupId(), 500)) {
                consumer.subscribe(Collections.singleton(KafkaHelper.TOPIC));
                // Force partition assignment before signaling ready.
                consumer.poll(Duration.ofMillis(200));
                consumerReady.countDown();

                int n = 0;
                while (n < COUNT) {
                    ConsumerRecords<String, String> recs = consumer.poll(Duration.ofSeconds(10));
                    if (recs.isEmpty()) {
                        System.err.println("Consumer poll timeout at " + n);
                        break;
                    }
                    long now = System.currentTimeMillis();
                    for (ConsumerRecord<String, String> r : recs) {
                        Header h = r.headers().lastHeader(TS_HEADER);
                        if (h != null) {
                            long ts = ByteBuffer.wrap(h.value()).getLong();
                            latencies.add(now - ts);
                        }
                        n++;
                        consumerDone.countDown();
                        if (n >= COUNT) break;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, "kafka-consumer");

        consumerThread.start();
        if (!consumerReady.await(15, TimeUnit.SECONDS)) {
            throw new RuntimeException("Consumer didn't become ready");
        }
        System.out.println("Consumer ready. Producing " + COUNT + " messages...");

        // ---------------------------- Producer ------------------------------
        Thread producerThread = new Thread(() -> {
            try (KafkaProducer<String, String> producer = KafkaHelper.createAsyncProducer()) {
                for (int i = 0; i < COUNT; i++) {
                    long now = System.currentTimeMillis();
                    byte[] tsBytes = ByteBuffer.allocate(8).putLong(now).array();
                    ProducerRecord<String, String> rec =
                            new ProducerRecord<>(KafkaHelper.TOPIC, null, KafkaHelper.PAYLOAD_1KB);
                    rec.headers().add(TS_HEADER, tsBytes);
                    producer.send(rec);            // async - let kafka batch internally
                }
                producer.flush();
                System.out.println("Producer done.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, "kafka-producer");

        producerThread.start();
        consumerDone.await(120, TimeUnit.SECONDS);
        consumerThread.join(5_000);
        producerThread.join(5_000);

        // ---------------------------- Summary -------------------------------
        List<Long> sorted = new ArrayList<>(latencies);
        Collections.sort(sorted);
        int sz = sorted.size();
        if (sz == 0) {
            System.err.println("ERROR: no latencies recorded.");
            System.exit(1);
        }
        long med = sz % 2 == 0
                 ? (sorted.get(sz/2 - 1) + sorted.get(sz/2)) / 2
                 : sorted.get(sz/2);

        System.out.println("\n=== SUMMARY (copy to report) ===");
        System.out.println("  Messages measured       : " + sz);
        System.out.println("  Kafka median latency    : " + med + " ms");
        System.out.println("  Kafka min latency       : " + sorted.get(0) + " ms");
        System.out.println("  Kafka max latency       : " + sorted.get(sz - 1) + " ms");
        System.out.println("  Kafka p95 latency       : " + sorted.get((int)(sz * 0.95)) + " ms");
    }
}
