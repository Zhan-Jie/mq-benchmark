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

public class TestCase implements ProducerListener, ConsumerListener {
    private int exchangesNumber;
    private int queuesPerExchange;
    private int producersPerExchange;
    private boolean autoAck = true;
    private boolean durable = false;
    private boolean mirrorQueues = false;
    private String confirmOrTx = "none";

    private int messagesPerProducer = 10000;

    private Connection connection;
    private List<Runnable> producers;
    private List<Runnable> consumers;

    private AtomicInteger delivered = new AtomicInteger(0);
    private final int totalMessages;
    private CountDownLatch latch;

    private ExecutorService executor;

    private long producerStartTime = Long.MAX_VALUE;
    private long consumerFinishedTime = 0L;

    public TestCase(int exchangesNumber, int queuesPerExchange, int producersPerExchange, Connection connection, CountDownLatch latch) {
        this.exchangesNumber = exchangesNumber;
        this.queuesPerExchange = queuesPerExchange;
        this.producersPerExchange = producersPerExchange;
        this.connection = connection;
        this.totalMessages = messagesPerProducer * (producersPerExchange*exchangesNumber);
        this.latch = latch;
        System.out.println("total: " + totalMessages);
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
            channel.exchangeDelete(ex);
            channel.exchangeDeclare(ex, BuiltinExchangeType.DIRECT, durable, false, null);
            for (int j = 0; j < queuesPerExchange; ++j) {
                String q = queuePrefix + (j + i*queuesPerExchange);
                channel.queueDelete(q);
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
                Runnable c = new Producer(connection.createChannel(), exchangePrefix + i, queues, messagesPerProducer, confirmOrTx, durable, this);
                producers.add(c);
            }
        }
    }

    public void start() {
        executor = Executors.newFixedThreadPool(consumers.size() + producers.size());
        for (Runnable r : producers) {
            executor.submit(r);
        }
        System.out.format("%d producers threads started...%n", producers.size());
        for (Runnable r : consumers) {
            executor.submit(r);
        }
        System.out.format("%d consumers threads started...%n", consumers.size());
    }

    public void stop() {
        executor.shutdownNow();
    }

    @Override
    public void onReceive() {
        if (delivered.incrementAndGet() >= totalMessages) {
            try {
                consumerFinishedTime = System.currentTimeMillis();
                connection.close();
                System.out.println("Close Connection~");
                latch.countDown();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void onSendFirstMessage(long time) {
        synchronized (this) {
            if (producerStartTime > time) {
                producerStartTime = time;
            }
        }
    }

    public String report () {
        long duration = consumerFinishedTime - producerStartTime;
        StringBuilder text = new StringBuilder("================ SUMMARY REPORT =================\n\n");
        text.append("Text Condition:\n");
        text.append("\tproducers:").append(producers.size()).append("\n");
        text.append("\texchanges:").append(exchangesNumber).append("\n");
        text.append("\tqueues/consumers:").append(consumers.size()).append("\n\n");

        text.append("\tauto-ack=").append(autoAck).append("\n");
        text.append("\tdurable=").append(durable).append("\n");
        text.append("\tmirror-queue=").append(mirrorQueues).append("\n");
        text.append("\tconfirm-mode=").append("confirm".equals(confirmOrTx)).append("\n");
        text.append("\ttx-mode=").append("tx".equals(confirmOrTx)).append("\n\n");

        text.append("PERFORMANCE:\n");
        text.append("\tExpected Total Messages: ").append(totalMessages).append("\n");
        text.append("\tActually Delivered Messages: ").append(delivered.get()).append("\n");
        text.append(String.format("\tTime Spent: %f seconds.%n", duration / 1000.0));
        text.append(String.format("\tMessage Rate: %f msg/sec. %n",delivered.get()*1000.0/duration));
        text.append("\n====================== END ===================");
        return text.toString();
    }
}
