package kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;

/**
 * Lab requirement A.1, Kafka side: median produce / consume response time
 * over 1000 calls. Methodology mirrors jms.ResponseTimeBenchmark exactly.
 *
 *  Produce phase: 1000 × 1 KB messages, time each producer.send().get().
 *                 Calling .get() makes the send synchronous, matching JMS's
 *                 blocking producer.send() semantics so the comparison is fair.
 *
 *  Consume phase: max.poll.records=1 forces poll() to return at most one
 *                 record at a time, mirroring JMS's per-message receive().
 *                 1000 polls measured.
 *
 *  Note: the FIRST call to send() includes broker metadata fetch overhead;
 *        the FIRST poll() includes group join + offset fetch. We do one
 *        warm-up call of each before timing to avoid those one-time costs
 *        skewing the median... actually they don't, since 1 outlier in 1000
 *        doesn't move the median. We don't warm up, to stay parallel with JMS.
 */
public class ResponseTimeBenchmark {

    private static final int RUNS = 1000;

    public static void main(String[] args) throws Exception {
        System.out.println("=== Kafka Response Time Benchmark ===");
        System.out.println("Bootstrap : " + KafkaHelper.bootstrap());
        System.out.println("Runs      : " + RUNS);
        System.out.println("Payload   : 1 KB\n");

        KafkaHelper.resetTopic();

        // ------------------------------ PRODUCE ------------------------------
        List<Long> produceTimesUs = new ArrayList<>(RUNS);
        try (KafkaProducer<String, String> producer = KafkaHelper.createProducer()) {
            for (int i = 0; i < RUNS; i++) {
                ProducerRecord<String, String> rec =
                        new ProducerRecord<>(KafkaHelper.TOPIC, KafkaHelper.PAYLOAD_1KB);
                long start = System.nanoTime();
                Future<RecordMetadata> f = producer.send(rec);
                f.get();                       // block on broker ack - matches JMS sync send
                long elapsedNs = System.nanoTime() - start;
                produceTimesUs.add(elapsedNs / 1000L);
            }
        }

        long pMedUs = median(produceTimesUs);
        System.out.println("--- Produce response time (per send().get() call) ---");
        System.out.println("  Median : " + pMedUs + " µs  (" + (pMedUs / 1000.0) + " ms)");
        System.out.println("  Min    : " + Collections.min(produceTimesUs) + " µs");
        System.out.println("  Max    : " + Collections.max(produceTimesUs) + " µs\n");

        // ------------------------------ CONSUME ------------------------------
        List<Long> consumeTimesUs = new ArrayList<>(RUNS);
        try (KafkaConsumer<String, String> consumer =
                     KafkaHelper.createConsumer(KafkaHelper.newGroupId(), 1)) {

            consumer.subscribe(Collections.singleton(KafkaHelper.TOPIC));
            int got = 0;
            while (got < RUNS) {
                long start = System.nanoTime();
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
                long elapsedNs = System.nanoTime() - start;
                if (records.isEmpty()) {
                    System.err.println("Consumer poll timeout at " + got);
                    break;
                }
                for (ConsumerRecord<String, String> ignored : records) {
                    consumeTimesUs.add(elapsedNs / 1000L);
                    got++;
                    if (got >= RUNS) break;
                }
            }
        }

        long cMedUs = median(consumeTimesUs);
        System.out.println("--- Consume response time (per poll() returning 1 record) ---");
        System.out.println("  Median : " + cMedUs + " µs  (" + (cMedUs / 1000.0) + " ms)");
        System.out.println("  Min    : " + Collections.min(consumeTimesUs) + " µs");
        System.out.println("  Max    : " + Collections.max(consumeTimesUs) + " µs\n");

        System.out.println("=== SUMMARY (copy to report) ===");
        System.out.println("  Kafka produce response time (median): " + pMedUs + " µs");
        System.out.println("  Kafka consume response time (median): " + cMedUs + " µs");
    }

    static long median(List<Long> values) {
        Collections.sort(values);
        int n = values.size();
        return n % 2 == 0 ? (values.get(n/2-1) + values.get(n/2)) / 2 : values.get(n/2);
    }
}
