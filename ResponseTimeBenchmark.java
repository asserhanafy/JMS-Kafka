package jms;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Lab requirement A.1: median produce / consume response time over 1000 calls.
 *
 *   Produce phase: send 1000 × 1 KB messages, time each producer.send() call.
 *   Consume phase: receive 1000 messages back from the queue, time each
 *                  consumer.receive() call.
 *
 * Times are recorded in MICROSECONDS (System.nanoTime / 1000) to give meaningful
 * precision; medians are reported in both µs and ms in the summary.
 */
public class ResponseTimeBenchmark {

    private static final int RUNS = 1000;

    public static void main(String[] args) throws Exception {
        System.out.println("=== JMS Response Time Benchmark ===");
        System.out.println("Broker URL : " + JMSHelper.brokerUrl());
        System.out.println("Runs       : " + RUNS);
        System.out.println("Payload    : 1 KB\n");

        Connection connection = JMSHelper.createConnection();
        Session session = JMSHelper.createSession(connection);
        Queue queue = JMSHelper.createQueue(session);

        int purged = JMSHelper.purgeQueue(session, queue);
        System.out.println("Purged " + purged + " stale messages from queue.\n");

        // ------------------------------ PRODUCE ------------------------------
        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        List<Long> produceTimesUs = new ArrayList<>(RUNS);

        for (int i = 0; i < RUNS; i++) {
            TextMessage msg = session.createTextMessage(JMSHelper.PAYLOAD_1KB);
            long start = System.nanoTime();
            producer.send(msg);                        // <-- the API call we're timing
            long elapsedNs = System.nanoTime() - start;
            produceTimesUs.add(elapsedNs / 1000L);
        }
        producer.close();

        long pMedUs = median(produceTimesUs);
        System.out.println("--- Produce response time (per send call) ---");
        System.out.println("  Median : " + pMedUs + " µs  (" + (pMedUs / 1000.0) + " ms)");
        System.out.println("  Min    : " + Collections.min(produceTimesUs) + " µs");
        System.out.println("  Max    : " + Collections.max(produceTimesUs) + " µs\n");

        // ------------------------------ CONSUME ------------------------------
        MessageConsumer consumer = session.createConsumer(queue);
        List<Long> consumeTimesUs = new ArrayList<>(RUNS);

        for (int i = 0; i < RUNS; i++) {
            long start = System.nanoTime();
            Message received = consumer.receive(5000);    // <-- API call we're timing
            long elapsedNs = System.nanoTime() - start;
            if (received == null) {
                System.err.println("Consumer timeout at iteration " + i);
                break;
            }
            consumeTimesUs.add(elapsedNs / 1000L);
        }
        consumer.close();
        session.close();
        connection.close();

        long cMedUs = median(consumeTimesUs);
        System.out.println("--- Consume response time (per receive call) ---");
        System.out.println("  Median : " + cMedUs + " µs  (" + (cMedUs / 1000.0) + " ms)");
        System.out.println("  Min    : " + Collections.min(consumeTimesUs) + " µs");
        System.out.println("  Max    : " + Collections.max(consumeTimesUs) + " µs\n");

        System.out.println("=== SUMMARY (copy to report) ===");
        System.out.println("  JMS produce response time (median): " + pMedUs + " µs");
        System.out.println("  JMS consume response time (median): " + cMedUs + " µs");
    }

    static long median(List<Long> values) {
        Collections.sort(values);
        int n = values.size();
        return n % 2 == 0 ? (values.get(n/2-1) + values.get(n/2)) / 2 : values.get(n/2);
    }
}
