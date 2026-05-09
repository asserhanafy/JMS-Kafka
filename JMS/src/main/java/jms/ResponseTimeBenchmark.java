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

public class ResponseTimeBenchmark {
    private static final int RUNS = 1000;

    public static void main(String[] args) throws Exception {
        System.out.println("=== JMS Response Time Benchmark ===");
        Connection connection = JMSHelper.createConnection();
        Session session = JMSHelper.createSession(connection);
        Queue queue = JMSHelper.createQueue(session);

        int purged = JMSHelper.purgeQueue(session, queue);
        System.out.println("Purged " + purged + " old messages.\n");

        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        List<Long> produceTimes = new ArrayList<>(RUNS);

        for (int i = 0; i < RUNS; i++) {
            TextMessage msg = session.createTextMessage(JMSHelper.PAYLOAD_1KB);
            long start = System.nanoTime();
            producer.send(msg);
            produceTimes.add((System.nanoTime() - start) / 1000); // microseconds
        }
        producer.close();

        System.out.println("--- Produce Response Time ---");
        System.out.println("  Median : " + median(produceTimes) + " µs");
        System.out.println("  Min    : " + Collections.min(produceTimes) + " µs");
        System.out.println("  Max    : " + Collections.max(produceTimes) + " µs\n");

        MessageConsumer consumer = session.createConsumer(queue);
        List<Long> consumeTimes = new ArrayList<>(RUNS);

        for (int i = 0; i < RUNS; i++) {
            long start = System.nanoTime();
            Message received = consumer.receive(5000);
            long elapsed = (System.nanoTime() - start) / 1000; // microseconds
            if (received == null) { System.err.println("Timeout at " + i); break; }
            consumeTimes.add(elapsed);
        }
        consumer.close();
        session.close();
        connection.close();

        System.out.println("--- Consume Response Time ---");
        System.out.println("  Median : " + median(consumeTimes) + " µs");
        System.out.println("  Min    : " + Collections.min(consumeTimes) + " µs");
        System.out.println("  Max    : " + Collections.max(consumeTimes) + " µs\n");

        System.out.println("=== SUMMARY ===");
        System.out.println("  Produce response time (median): " + median(produceTimes) + " µs");
        System.out.println("  Consume response time (median): " + median(consumeTimes) + " µs");
    }

    static long median(List<Long> values) {
        Collections.sort(values);
        int n = values.size();
        return n % 2 == 0 ? (values.get(n/2-1) + values.get(n/2)) / 2 : values.get(n/2);
    }
}
