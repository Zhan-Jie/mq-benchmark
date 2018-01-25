package zhanj;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class Prepare {
    private int exchangesNumber;
    private int queuesPerExchange;
    private boolean durable;
    private boolean mirrorQueues;

    private List<Connection> connections;
    private int nextConn = 0;

    public Prepare(int exchangesNumber, int queuesPerExchange, boolean durable, boolean mirrorQueues, List<Connection> connections) {
        this.exchangesNumber = exchangesNumber;
        this.queuesPerExchange = queuesPerExchange;
        this.durable = durable;
        this.mirrorQueues = mirrorQueues;
        this.connections = connections;
    }

    public void prepare () throws IOException, TimeoutException {
        String queuePrefix = mirrorQueues ? "mirror-q-" : "q-";
        String exchangePrefix = "ex-";

        for (int i = 0; i < exchangesNumber; ++i) {
            Channel channel = getConnection().createChannel();
            String ex = exchangePrefix + i;
            channel.exchangeDelete(ex);
            channel.exchangeDeclare(ex, BuiltinExchangeType.DIRECT, durable, false, null);
            channel.close();
        }
        for (int i = 0; i < exchangesNumber; ++i) {
            String ex = exchangePrefix + i;
            for (int j = 0; j < queuesPerExchange; ++j) {
                Channel channel = getConnection().createChannel();
                String q = queuePrefix + (j + i*queuesPerExchange);
                channel.queueDelete(q);
                channel.queueDeclare(q, durable, false, false, null);
                channel.queueBind(q, ex, q);
                channel.close();
            }
        }
    }

    private Connection getConnection() {
        Connection conn = connections.get(nextConn);
        nextConn = (nextConn + 1) % connections.size();
        return conn;
    }
}
