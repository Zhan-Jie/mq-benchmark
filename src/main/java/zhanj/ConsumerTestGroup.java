package zhanj;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class ConsumerTestGroup implements ConsumerListener{
    private int exchangesNumber;
    private int queuesPerExchange;
    private boolean autoAck;
    private boolean mirrorQueues;
    private final int totalMessages;

    private List<Connection> connections;
    private List<Runnable> consumers;
    private AtomicInteger delivered = new AtomicInteger(0);
    private long consumerFinishedTime = 0L;
    private int nextConn = 0;
    private CountDownLatch latch;

    private ExecutorService executor;

    public ConsumerTestGroup(int exchangesNumber, int queuesPerExchange, boolean autoAck, boolean mirrorQueues, int totalMessages, List<Connection> connections, CountDownLatch latch) {
        this.exchangesNumber = exchangesNumber;
        this.queuesPerExchange = queuesPerExchange;
        this.autoAck = autoAck;
        this.mirrorQueues = mirrorQueues;
        this.totalMessages = totalMessages;
        this.connections = connections;
        this.latch = latch;
    }

    public void initialize () throws IOException {
        String queuePrefix = mirrorQueues ? "mirror-q-" : "q-";

        consumers = new ArrayList<>(exchangesNumber*queuesPerExchange);
        for (int i = 0; i < exchangesNumber; ++i) {
            for (int k = 0; k < queuesPerExchange; ++k) {
                Runnable c = new Consumer(getConnection().createChannel(), queuePrefix + (k + i*queuesPerExchange), autoAck, this);
                consumers.add(c);
            }
        }
    }

    private Connection getConnection () {
        Connection conn = connections.get(nextConn);
        nextConn = (nextConn + 1) % connections.size();
        return conn;
    }

    @Override
    public void onReceive() {
        if (delivered.incrementAndGet() >= totalMessages) {
            consumerFinishedTime = System.currentTimeMillis();
            latch.countDown();
        }
    }

    public void start () {
        executor = Executors.newFixedThreadPool(consumers.size());
        for (Runnable r : consumers) {
            executor.submit(r);
        }
        System.out.format("%d consumers threads started...%n", consumers.size());
    }

    public void stop() {
        executor.shutdownNow();
    }

    public String report () throws IOException {
        StringWriter writer = new StringWriter();
        JsonGenerator gen = new JsonFactory().createGenerator(writer);
        gen.useDefaultPrettyPrinter();
        gen.writeStartObject();
        gen.writeStringField("type", "consumer");
        gen.writeBooleanField("autoAck", autoAck);
        gen.writeBooleanField("mirror", mirrorQueues);
        gen.writeNumberField("consumers", consumers.size());
        gen.writeNumberField("received", delivered.get());
        gen.writeNumberField("endTime", consumerFinishedTime);
        gen.writeEndObject();
        gen.close();
        return writer.toString();
    }
}
