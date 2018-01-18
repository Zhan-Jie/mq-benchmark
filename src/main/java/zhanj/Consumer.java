package zhanj;


import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class Consumer extends DefaultConsumer implements Runnable{
    private Channel channel;
    private String name;

    private int count = 0;

    private long begin;

    private boolean autoAck;
    private OnReceiveMessageListener listener;

    public Consumer(Channel channel, String queueName, boolean autoAck, OnReceiveMessageListener listener) {
        super(channel);
        this.channel = channel;
        this.name = queueName;
        this.autoAck = autoAck;
        this.listener = listener;
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        ++count;
        if (!autoAck) {
            channel.basicAck(envelope.getDeliveryTag(), false);
        }
        listener.onReceive();
    }

    private void receiveMessage () throws IOException{
        begin = System.currentTimeMillis();
        System.out.format("[%d] consumer %s is listening to queue %s...%n", begin, name, name);
        channel.basicConsume(name, autoAck, this);
    }

    @Override
    public String toString() {
        return String.format("consumer [%s] received %d messages", name, count);
    }

    @Override
    public void run() {
        try {
            receiveMessage();
        } catch (IOException e) {
            e.printStackTrace();
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException | TimeoutException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }

    @Override
    public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
        super.handleShutdownSignal(consumerTag, sig);
        long now = System.currentTimeMillis();
        System.out.format("[%d]consumer %s exit. used %d milliseconds.%n", now, name, now-begin);

    }
}
