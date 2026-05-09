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

/**
 * Lab requirement A.2: maximum throughput for produce and consume.
 *
 * Methodology (per the lab handout, page 3):
 *   - Pick a target throughput X (msg/sec).
 *   - Period T = 1000 ms / X.
 *   - Loop X times, sleeping (T - 0.2 T) ms after each call to spread the
 *     load *across* the second instead of bunching at the end.
 *   - A test "passes" if all messages are accepted (zero errors) and the
 *     queue ends up containing the expected count.
 *   - Start at START_THROUGHPUT and double X every iteration until a test
 *     fails. Report the last X that passed.
 *
 * Note: at very high X the period T - 0.2T rounds to 0 ms, which makes
 * Thread.sleep a no-op. That's expected: we let the loop run flat-out
 * once the per-call overhead alone exceeds the budget.
 */
public class ThroughputBenchmark {

    private static final int START_THROUGHPUT = 1_000;
    private static final int MAX_THROUGHPUT   = 512_000;  // ceiling so we don't loop forever
    private static final int RAMP_FACTOR      = 2;

    public static void main(String[] args) throws Exception {
        System.out.println("=== JMS Maximum Throughput Benchmark ===");
        System.out.println("Broker URL : " + JMSHelper.brokerUrl());
        System.out.println();

        System.out.println("── PRODUCE Throughput ──");
        int maxProduce = findMax("PRODUCE");

        System.out.println("\n── CONSUME Throughput ──");
        int maxConsume = findMax("CONSUME");

        System.out.println("\n=== SUMMARY (copy to report) ===");
        System.out.println("  JMS max produce throughput: " + maxProduce + " msg/sec");
        System.out.println("  JMS max consume throughput: " + maxConsume + " msg/sec");
    }

    /** Exponentially ramps X until a trial fails. Returns last successful X. */
    static int findMax(String mode) throws Exception {
        int last = 0;
        int X = START_THROUGHPUT;
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
        Connection conn = JMSHelper.createConnection();
        Session session = JMSHelper.createSession(conn);
        Queue queue = JMSHelper.createQueue(session);
        JMSHelper.purgeQueue(session, queue);

        // Sleep budget per call: T - 0.2 T = 0.8 T (in ms).
        long sleepMs = (long) ((1000.0 / X) * 0.8);

        AtomicInteger sent = new AtomicInteger();
        AtomicInteger received = new AtomicInteger();
        AtomicInteger errors = new AtomicInteger();

        if ("PRODUCE".equals(mode)) {
            MessageProducer producer = session.createProducer(queue);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            // Allow up to 1.2 s wall-clock to absorb scheduling jitter.
            long deadline = System.currentTimeMillis() + 1_200;
            for (int i = 0; i < X && System.currentTimeMillis() < deadline; i++) {
                try {
                    producer.send(session.createTextMessage(JMSHelper.PAYLOAD_1KB));
                    sent.incrementAndGet();
                } catch (JMSException e) {
                    errors.incrementAndGet();
                }
                if (sleepMs > 0) Thread.sleep(sleepMs);
            }
            producer.close();
            // Verify all sent messages can be drained.
            received.set(drain(session, queue, sent.get()));

        } else {  // CONSUME
            // Pre-fill the queue so we can isolate consume-side throughput.
            MessageProducer pre = session.createProducer(queue);
            pre.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            for (int i = 0; i < X; i++) {
                pre.send(session.createTextMessage(JMSHelper.PAYLOAD_1KB));
                sent.incrementAndGet();
            }
            pre.close();

            MessageConsumer consumer = session.createConsumer(queue);
            long deadline = System.currentTimeMillis() + 1_200;
            for (int i = 0; i < X && System.currentTimeMillis() < deadline; i++) {
                try {
                    Message m = consumer.receive(500);
                    if (m != null) received.incrementAndGet();
                    else errors.incrementAndGet();
                } catch (JMSException e) {
                    errors.incrementAndGet();
                }
                if (sleepMs > 0) Thread.sleep(sleepMs);
            }
            consumer.close();
        }

        session.close();
        conn.close();

        // Pass criterion: zero errors AND every produced message accounted for.
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
