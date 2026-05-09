package jms;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;

public class JMSHelper {
    public static final String BROKER_URL  = "vm://localhost?broker.persistent=false";
    public static final String QUEUE_NAME  = "benchmark.queue";
    public static final String PAYLOAD_1KB = "A".repeat(1024);

    public static Connection createConnection() throws JMSException {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
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

    public static int purgeQueue(Session session, Queue queue) throws JMSException {
        MessageConsumer consumer = session.createConsumer(queue);
        int count = 0;
        Message m;
        while ((m = consumer.receive(200)) != null) count++;
        consumer.close();
        return count;
    }
}
