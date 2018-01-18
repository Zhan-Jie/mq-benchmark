package zhanj;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class Producer implements Runnable{
    private Channel channel;
    private String name;

    private int count = 0;
    private final int total;
    private byte[] message;
    private List<String> queues;

    private String txOrConfirm;
    private AMQP.BasicProperties props = null;

    private long begin;

    public Producer(Channel channel, String exName, List<String> queues, int total, String txOrConfirm, boolean durable) throws IOException {
        this.channel = channel;
        this.name = exName;
        this.total = total;
        this.message = ("sent from exchange: " + exName).getBytes();
        this.queues = queues;
        this.txOrConfirm = txOrConfirm;
        if (durable) {
            props = new AMQP.BasicProperties.Builder().deliveryMode(2).build();
        }
    }

    private void sendMessage (byte[] message, String queue) throws IOException {
        channel.basicPublish(name, queue, props, message);
        ++count;
    }

    private void sendMessageWithConfirm (byte[] message, String queue) throws IOException, InterruptedException {
        channel.confirmSelect();
        channel.basicPublish(name, queue, props, message);
        channel.waitForConfirmsOrDie();
    }

    private void sendMessageWithTx (byte[] message, String queue) throws IOException {
        channel.txSelect();
        channel.basicPublish(name, queue, props, message);
        channel.txCommit();
    }

    @Override
    public String toString() {
        return String.format("producer [%s] sent %d messages", name, count);
    }

    public void run() {
        begin = System.currentTimeMillis();
        System.out.format("[%d] producer %s is sending messages to exchange %s...%n", begin, name, name);

        int s = queues.size();
        try {
            if ("none".equals(txOrConfirm)) {
                for (int i = 0; i < total; ++i) {
                    sendMessage(message, queues.get(i % s));
                }
            } else if ("confirm".equals(txOrConfirm)) {
                for (int i = 0; i < total; ++i) {
                    sendMessageWithConfirm(message, queues.get(i % s));
                }
            } else if ("tx".equals(txOrConfirm)) {
                for (int i = 0; i < total; ++i) {
                    sendMessageWithTx(message, queues.get(i % s));
                }
            } else {
                System.err.println("unknown txOrConfirm: " + txOrConfirm);
            }

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            try {
                if (channel != null) {
                    channel.close();
                }
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
            }
            channel = null;
            long now = System.currentTimeMillis();
            App.decrement();
            System.out.format("[%d] producer %s stopped. used %d milliseconds.%n", now, name, now - begin);
        }
    }
}
