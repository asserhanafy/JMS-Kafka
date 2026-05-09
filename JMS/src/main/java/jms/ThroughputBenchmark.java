package jms;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.concurrent.atomic.AtomicInteger;

public class ThroughputBenchmark {
    private static final int START_THROUGHPUT = 100;
    private static final int MAX_THROUGHPUT   = 400_000;

    public static void main(String[] args) throws Exception {
        System.out.println("=== JMS Maximum Throughput Benchmark ===\n");
        System.out.println("── PRODUCE Throughput ──");
        int maxProduce = findMax("PRODUCE");
        System.out.println("\n── CONSUME Throughput ──");
        int maxConsume = findMax("CONSUME");
        System.out.println("\n=== SUMMARY ===");
        System.out.println("  Max Produce throughput: " + maxProduce + " msg/sec");
        System.out.println("  Max Consume throughput: " + maxConsume + " msg/sec");
    }

    static int findMax(String mode) throws Exception {
        int last = 0, X = START_THROUGHPUT;
        while (X <= MAX_THROUGHPUT) {
            boolean ok = runTrial(X, mode);
            System.out.printf("  X=%6d msg/sec -> %s%n", X, ok ? "OK" : "FAILED");
            if (!ok) break;
            last = X;
            X *= 2;
        }
        System.out.printf("  -> Max %s throughput: %d msg/sec%n", mode, last);
        return last;
    }

    static boolean runTrial(int X, String mode) throws Exception {
        Connection conn = JMSHelper.createConnection();
        Session session = JMSHelper.createSession(conn);
        Queue queue = JMSHelper.createQueue(session);
        JMSHelper.purgeQueue(session, queue);

        long sleepMs = (long)((1000.0 / X) * 0.8);
        AtomicInteger sent = new AtomicInteger();
        AtomicInteger received = new AtomicInteger();
        AtomicInteger errors = new AtomicInteger();

        if ("PRODUCE".equals(mode)) {
            MessageProducer producer = session.createProducer(queue);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            long deadline = System.currentTimeMillis() + 1200;
            for (int i = 0; i < X && System.currentTimeMillis() < deadline; i++) {
                try {
                    producer.send(session.createTextMessage(JMSHelper.PAYLOAD_1KB));
                    sent.incrementAndGet();
                } catch (JMSException e) { errors.incrementAndGet(); }
                if (sleepMs > 0) Thread.sleep(sleepMs);
            }
            producer.close();
            received.set(drain(session, queue, sent.get()));
        } else {
            MessageProducer pre = session.createProducer(queue);
            pre.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            for (int i = 0; i < X; i++) {
                pre.send(session.createTextMessage(JMSHelper.PAYLOAD_1KB));
                sent.incrementAndGet();
            }
            pre.close();
            MessageConsumer consumer = session.createConsumer(queue);
            long deadline = System.currentTimeMillis() + 1200;
            for (int i = 0; i < X && System.currentTimeMillis() < deadline; i++) {
                try {
                    Message m = consumer.receive(500);
                    if (m != null) received.incrementAndGet();
                    else errors.incrementAndGet();
                } catch (JMSException e) { errors.incrementAndGet(); }
                if (sleepMs > 0) Thread.sleep(sleepMs);
            }
            consumer.close();
        }
        session.close();
        conn.close();
        return errors.get() == 0 && received.get() >= sent.get();
    }

    static int drain(Session session, Queue queue, int expected) throws JMSException {
        MessageConsumer c = session.createConsumer(queue);
        int n = 0;
        while (n < expected && c.receive(300) != null) n++;
        c.close();
        return n;
    }
}
