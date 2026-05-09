package kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

/**
 * Shared boilerplate for all Kafka benchmarks.
 *
 *  Bootstrap-servers selection (in order of priority):
 *    1. System property -DbootstrapServers=...
 *    2. Environment variable KAFKA_BOOTSTRAP
 *    3. Default: localhost:9092 (matches docker-compose.yml)
 *
 *  Acks setting: "1" - broker leader confirms write before .send().get() returns.
 *  This is the closest analog to ActiveMQ's NON_PERSISTENT + AUTO_ACKNOWLEDGE
 *  (sync send, no fsync) used in the JMS benchmarks. Apples-to-apples.
 */
public class KafkaHelper {

    public static final String DEFAULT_BOOTSTRAP = "localhost:9092";
    public static final String TOPIC = "benchmark-topic";

    /** 1 KB payload of literal 'A' characters, as required by the lab. */
    public static final String PAYLOAD_1KB = "A".repeat(1024);

    public static String bootstrap() {
        String prop = System.getProperty("bootstrapServers");
        if (prop != null && !prop.isEmpty()) return prop;
        String env = System.getenv("KAFKA_BOOTSTRAP");
        if (env != null && !env.isEmpty()) return env;
        return DEFAULT_BOOTSTRAP;
    }

    /**
     * SYNC-friendly producer for response-time tests:
     *   linger=0, batch=0 → every .send() is its own request. Combined with
     *   .get() in the caller, this measures per-call response time honestly.
     */
    public static KafkaProducer<String, String> createProducer() {
        Properties p = baseProducerProps();
        p.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        p.put(ProducerConfig.BATCH_SIZE_CONFIG, 0);
        return new KafkaProducer<>(p);
    }

    /**
     * BATCHED producer for throughput / latency tests:
     *   default linger and batch.size let Kafka coalesce multiple records into
     *   one network request - this is how Kafka is designed to be used at
     *   high message rates. Always paired with .flush() at end of test window.
     */
    public static KafkaProducer<String, String> createAsyncProducer() {
        Properties p = baseProducerProps();
        p.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        p.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        return new KafkaProducer<>(p);
    }

    private static Properties baseProducerProps() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // acks=1: leader confirms write but no fsync, no replicas waited on.
        // Closest analog to ActiveMQ NON_PERSISTENT + AUTO_ACKNOWLEDGE.
        p.put(ProducerConfig.ACKS_CONFIG, "1");
        return p;
    }

    /**
     * @param maxPollRecords cap returned records per poll. Pass 1 for the
     *                        response-time benchmark (per-message granularity);
     *                        pass a larger number (e.g. 500) for throughput tests.
     */
    public static KafkaConsumer<String, String> createConsumer(String groupId, int maxPollRecords) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Always start fresh - benchmarks should be repeatable.
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Manual commit to avoid background commit interfering with timing.
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        // Don't wait around if records aren't immediately available.
        p.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
        return new KafkaConsumer<>(p);
    }

    /** Random group id - every benchmark consumer starts at the beginning of the topic. */
    public static String newGroupId() {
        return "bench-" + UUID.randomUUID();
    }

    /** Delete and recreate the topic so each run starts from a clean slate. */
    public static void resetTopic() throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap());
        try (AdminClient admin = AdminClient.create(p)) {
            try {
                admin.deleteTopics(Collections.singleton(TOPIC)).all().get();
                Thread.sleep(800);  // brief settle so creation doesn't race deletion
            } catch (Exception e) {
                // It's fine if the topic didn't exist (UnknownTopicOrPartitionException).
                if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                    // Unexpected - log but continue; topic will be auto-created on first send.
                    System.err.println("[resetTopic] delete: " + e.getMessage());
                }
            }
            try {
                admin.createTopics(Collections.singleton(
                        new NewTopic(TOPIC, 1, (short) 1))).all().get();
            } catch (Exception e) {
                // Already exists / auto-create raced us - fine.
            }
        }
    }
}
