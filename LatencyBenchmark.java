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

/**
 * Lab requirement A.3: median end-to-end latency
 * (time between produce and consume, per message).
 *
 *   1. Spin up a consumer that's already polling the queue.
 *   2. Producer attaches System.currentTimeMillis() as a JMS LongProperty "ts".
 *   3. On receive, latency = now() - msg.getLongProperty("ts").
 *   4. Repeat 10,000 times. Report median.
 *
 *  Both producer and consumer use NON_PERSISTENT mode (no fsync), matching
 *  the throughput and response-time benchmarks for consistency.
 */
public class LatencyBenchmark {

    private static final int COUNT = 10_000;

    public static void main(String[] args) throws Exception {
        System.out.println("=== JMS Median Latency Benchmark ===");
        System.out.println("Broker URL : " + JMSHelper.brokerUrl());
        System.out.println("Messages   : " + COUNT + "\n");

        List<Long> latencies = new CopyOnWriteArrayList<>();
        CountDownLatch consumerReady = new CountDownLatch(1);
        CountDownLatch consumerDone  = new CountDownLatch(COUNT);

        // ---------------------------- Consumer ------------------------------
        Thread consumerThread = new Thread(() -> {
            try {
                Connection conn = JMSHelper.createConnection();
                Session session = JMSHelper.createSession(conn);
                Queue queue = JMSHelper.createQueue(session);
                JMSHelper.purgeQueue(session, queue);

                MessageConsumer mc = session.createConsumer(queue);
                consumerReady.countDown();

                int n = 0;
                while (n < COUNT) {
                    Message m = mc.receive(10_000);
                    if (m == null) {
                        System.err.println("Consumer timeout at message " + n);
                        break;
                    }
                    long latencyMs = System.currentTimeMillis() - m.getLongProperty("ts");
                    latencies.add(latencyMs);
                    n++;
                    consumerDone.countDown();
                }
                mc.close();
                session.close();
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, "jms-consumer");

        consumerThread.start();
        if (!consumerReady.await(10, TimeUnit.SECONDS)) {
            throw new RuntimeException("Consumer didn't become ready");
        }
        System.out.println("Consumer ready. Producing " + COUNT + " messages...");

        // ---------------------------- Producer ------------------------------
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
                mp.close();
                session.close();
                conn.close();
                System.out.println("Producer done.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, "jms-producer");

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
        System.out.println("  Messages measured     : " + sz);
        System.out.println("  JMS median latency    : " + med + " ms");
        System.out.println("  JMS min latency       : " + sorted.get(0) + " ms");
        System.out.println("  JMS max latency       : " + sorted.get(sz - 1) + " ms");
        System.out.println("  JMS p95 latency       : " + sorted.get((int)(sz * 0.95)) + " ms");
    }
}
