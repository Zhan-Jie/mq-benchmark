package zhanj;

import com.rabbitmq.client.*;

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

    private int type;
    private AMQP.BasicProperties props = null;

    private ProducerListener listener;

    public Producer(Channel channel, String exName, List<String> queues, int total, String txOrConfirm, boolean durable, ProducerListener listener) throws IOException {
        this.channel = channel;
        this.name = exName;
        this.total = total;
        this.message = ("sent from exchange: " + exName).getBytes();
        this.queues = queues;
        if ("confirm".equals(txOrConfirm)) {
            type = 1;
        } else if ("tx".equals(txOrConfirm)) {
            type = 2;
        } else {
            type = 0;
        }
        if (durable) {
            props = new AMQP.BasicProperties.Builder().deliveryMode(2).build();
        }
        this.channel.addReturnListener(new ReturnListener() {
            @Override
            public void handleReturn(int replyCode, String replyText, String exchange, String routingKey, AMQP.BasicProperties properties, byte[] body) throws IOException {
                try {
                    System.out.println("message return. send again.");
                    sendMessage(body, routingKey, type);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        this.listener = listener;
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

    private void sendMessage (byte[] message, String queue, int type) throws IOException, InterruptedException {
        switch (type) {
            case 0:
                sendMessage(message, queue);
                break;
            case 1:
                sendMessageWithConfirm(message, queue);
                break;
            case 2:
                sendMessageWithTx(message, queue);
                break;
        }
    }

    @Override
    public String toString() {
        return String.format("producer [%s] sent %d messages", name, count);
    }

    public void run() {
        int s = queues.size();
        try {
            listener.onSendFirstMessage(System.currentTimeMillis());
            for (int i = 0; i < total; ++i) {
                sendMessage(message, queues.get(i % s), type);
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
        }
    }
}
