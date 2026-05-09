package jms;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;

/**
 * Shared boilerplate for all JMS benchmarks.
 *
 *  Broker URL selection (in order of priority):
 *    1. System property -DbrokerUrl=...
 *    2. Environment variable BROKER_URL
 *    3. Default: vm://localhost?broker.persistent=false  (embedded, no install)
 *
 *  To benchmark against a *real* TCP broker (more realistic numbers), do:
 *      BROKER_URL=tcp://localhost:61616 java -cp ... jms.ResponseTimeBenchmark
 *  after starting an ActiveMQ broker on that port. See README for instructions.
 */
public class JMSHelper {

    public static final String DEFAULT_BROKER_URL =
            "vm://localhost?broker.persistent=false";

    public static final String QUEUE_NAME  = "benchmark.queue";

    /** 1 KB payload of literal 'A' characters, as required by the lab. */
    public static final String PAYLOAD_1KB = "A".repeat(1024);

    public static String brokerUrl() {
        String prop = System.getProperty("brokerUrl");
        if (prop != null && !prop.isEmpty()) return prop;
        String env = System.getenv("BROKER_URL");
        if (env != null && !env.isEmpty()) return env;
        return DEFAULT_BROKER_URL;
    }

    public static Connection createConnection() throws JMSException {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl());
        // Disable client-side message bundling for fairer per-message timing.
        factory.setUseAsyncSend(false);
        Connection connection = factory.createConnection();
        connection.start();
        return connection;
    }

    public static Session createSession(Connection connection) throws JMSException {
        return connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    public static Queue createQueue(Session session) throws JMSException {
        return session.createQueue(QUEUE_NAME);
    }

    /** Drain the queue. Returns count of drained messages. */
    public static int purgeQueue(Session session, Queue queue) throws JMSException {
        MessageConsumer consumer = session.createConsumer(queue);
        int count = 0;
        Message m;
        while ((m = consumer.receive(200)) != null) count++;
        consumer.close();
        return count;
    }
}
