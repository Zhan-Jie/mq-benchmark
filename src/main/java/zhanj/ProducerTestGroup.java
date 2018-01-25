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

public class ProducerTestGroup implements ProducerListener {
    private int exchangesNumber;
    private int queuesPerExchange;
    private int producersPerExchange;
    private boolean durable;
    private boolean mirrorQueues;
    private String confirmOrTx;

    private int messagesPerProducer;

    private List<Connection> connections;
    private List<Runnable> producers;

    private ExecutorService executor;

    private long producerStartTime = Long.MAX_VALUE;
    private int nextConn = 0;
    private CountDownLatch latch;

    public ProducerTestGroup(int exchangesNumber, int queuesPerExchange, int producersPerExchange, boolean durable, boolean mirrorQueues, String confirmOrTx, int messagesPerProducer, List<Connection> connections) {
        this.exchangesNumber = exchangesNumber;
        this.queuesPerExchange = queuesPerExchange;
        this.producersPerExchange = producersPerExchange;
        this.durable = durable;
        this.mirrorQueues = mirrorQueues;
        this.confirmOrTx = confirmOrTx;
        this.messagesPerProducer = messagesPerProducer;
        this.connections = connections;
        this.latch = new CountDownLatch(exchangesNumber*producersPerExchange);
    }

    public void initialize () throws IOException {
        String queuePrefix = mirrorQueues ? "mirror-q-" : "q-";
        String exchangePrefix = "ex-";

        producers = new ArrayList<>(exchangesNumber*producersPerExchange);
        for (int i = 0; i < exchangesNumber; ++i) {
            List<String> queues = new ArrayList<>(queuesPerExchange);
            for (int k = 0; k < queuesPerExchange; ++k) {
                queues.add(queuePrefix + (k + i*queuesPerExchange));
            }
            for (int j = 0; j < producersPerExchange; ++j) {
                Runnable c = new Producer(getConnection().createChannel(), exchangePrefix + i, queues, messagesPerProducer, confirmOrTx, durable, this, latch);
                producers.add(c);
            }
        }
    }

    public void start(){
        executor = Executors.newFixedThreadPool(producers.size());
        for (Runnable r : producers) {
            executor.submit(r);
        }
        System.out.format("%d producers threads started...%n", producers.size());
        try {
            latch.await();
            System.out.println("All messages are sent out.");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public void stop() {
        executor.shutdownNow();
    }

    @Override
    public void onSendFirstMessage(long time) {
        synchronized (this) {
            if (producerStartTime > time) {
                producerStartTime = time;
            }
        }
    }

    private Connection getConnection () {
        Connection conn = connections.get(nextConn);
        nextConn = (nextConn + 1) % connections.size();
        return conn;
    }

    public void report () {
        System.out.println("\n======= PRODUCER REPORT =======");
        System.out.format("\t%d producers, %d exchanges, %d queues/consumers. %d messages in total.%n", producers.size(), exchangesNumber, queuesPerExchange*exchangesNumber, messagesPerProducer*producers.size());
        System.out.println("\tFirst message was sent at: " + producerStartTime + "\n");
    }
}
