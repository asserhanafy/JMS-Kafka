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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class LatencyBenchmark {
    private static final int COUNT = 10_000;

    public static void main(String[] args) throws Exception {
        System.out.println("=== JMS Median Latency Benchmark ===");
        List<Long> latencies = new CopyOnWriteArrayList<>();
        CountDownLatch ready = new CountDownLatch(1);
        CountDownLatch done  = new CountDownLatch(COUNT);

        Thread consumerThread = new Thread(() -> {
            try {
                Connection conn = JMSHelper.createConnection();
                Session session = JMSHelper.createSession(conn);
                Queue queue = JMSHelper.createQueue(session);
                JMSHelper.purgeQueue(session, queue);
                MessageConsumer mc = session.createConsumer(queue);
                ready.countDown();
                int n = 0;
                while (n < COUNT) {
                    Message m = mc.receive(10_000);
                    if (m == null) { System.err.println("Timeout at " + n); break; }
                    latencies.add(System.currentTimeMillis() - m.getLongProperty("ts"));
                    n++;
                    done.countDown();
                }
                mc.close(); session.close(); conn.close();
            } catch (Exception e) { e.printStackTrace(); }
        });
        consumerThread.start();
        ready.await(10, TimeUnit.SECONDS);
        System.out.println("Consumer ready, producing " + COUNT + " messages...");

        Thread producerThread = new Thread(() -> {
            try {
                Connection conn = JMSHelper.createConnection();
                Session session = JMSHelper.createSession(conn);
                Queue queue = JMSHelper.createQueue(session);
                MessageProducer mp = session.createProducer(queue);
                mp.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                for (int i = 0; i < COUNT; i++) {
                    TextMessage m = session.createTextMessage(JMSHelper.PAYLOAD_1KB);
                    m.setLongProperty("ts", System.currentTimeMillis());
                    mp.send(m);
                }
                mp.close(); session.close(); conn.close();
                System.out.println("Producer done.");
            } catch (Exception e) { e.printStackTrace(); }
        });
        producerThread.start();

        done.await(120, TimeUnit.SECONDS);
        consumerThread.join(5000);
        producerThread.join(5000);

        List<Long> sorted = new ArrayList<>(latencies);
        Collections.sort(sorted);
        int sz = sorted.size();
        long med = sz % 2 == 0 ? (sorted.get(sz/2-1) + sorted.get(sz/2)) / 2 : sorted.get(sz/2);

        System.out.println("\n=== SUMMARY ===");
        System.out.println("  Messages measured : " + sz);
        System.out.println("  Median latency    : " + med + " ms");
        System.out.println("  Min latency       : " + sorted.get(0) + " ms");
        System.out.println("  Max latency       : " + sorted.get(sz-1) + " ms");
    }
}
