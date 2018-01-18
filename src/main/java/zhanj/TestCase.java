package zhanj;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class TestCase implements OnReceiveMessageListener {
    private int exchangesNumber;
    private int queuesPerExchange;
    private int producersPerExchange;
    private boolean autoAck = true;
    private boolean durable = false;
    private boolean mirrorQueues = false;
    private String confirmOrTx = "none";

    private int messagesPerProducer = 100000;

    private Connection connection;
    private List<Runnable> producers;
    private List<Runnable> consumers;

    private AtomicInteger delivered = new AtomicInteger(0);
    private final int totalMessages;
    private CountDownLatch latch;

    public TestCase(int exchangesNumber, int queuesPerExchange, int producersPerExchange, Connection connection, CountDownLatch latch) {
        this.exchangesNumber = exchangesNumber;
        this.queuesPerExchange = queuesPerExchange;
        this.producersPerExchange = producersPerExchange;
        this.connection = connection;
        this.totalMessages = messagesPerProducer * (producersPerExchange*exchangesNumber);
        this.latch = latch;
    }

    public void setAutoAck(boolean autoAck) {
        this.autoAck = autoAck;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public void setMirrorQueues(boolean mirrorQueues) {
        this.mirrorQueues = mirrorQueues;
    }

    public void setConfirmOrTx(String confirmOrTx) {
        this.confirmOrTx = confirmOrTx;
    }

    public void initialize () throws IOException, TimeoutException {
        Channel channel = connection.createChannel();

        String queuePrefix = mirrorQueues ? "mirror-q-" : "q-";
        String exchangePrefix = "ex-";

        for (int i = 0; i < exchangesNumber; ++i) {
            String ex = exchangePrefix + i;
            channel.exchangeDeclare(ex, BuiltinExchangeType.DIRECT, durable, false, null);
            for (int j = 0; j < queuesPerExchange; ++j) {
                String q = queuePrefix + (j + i*queuesPerExchange);
                channel.queueDeclare(q, durable, false, false, null);
                channel.queueBind(q, ex, q);
            }
        }
        channel.close();

        producers = new ArrayList<>(exchangesNumber*producersPerExchange);
        consumers = new ArrayList<>(exchangesNumber*queuesPerExchange);
        for (int i = 0; i < exchangesNumber; ++i) {
            List<String> queues = new ArrayList<>(queuesPerExchange);
            for (int k = 0; k < queuesPerExchange; ++k) {
                queues.add(queuePrefix + (k + i*queuesPerExchange));
                Runnable c = new Consumer(connection.createChannel(), queuePrefix + (k + i*queuesPerExchange), autoAck, this);
                consumers.add(c);
            }
            for (int j = 0; j < producersPerExchange; ++j) {
                Runnable c = new Producer(connection.createChannel(), exchangePrefix + (j + i*producersPerExchange), queues, messagesPerProducer, confirmOrTx, durable);
                producers.add(c);
            }
        }
    }

    public void start() {
        ExecutorService executor = Executors.newFixedThreadPool(consumers.size() + producers.size());
        for (Runnable r : producers) {
            executor.submit(r);
        }
        for (Runnable r : consumers) {
            executor.submit(r);
        }
    }

    @Override
    public void onReceive() {
        if (delivered.incrementAndGet() >= totalMessages) {
            try {
                connection.close();
                latch.countDown();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
