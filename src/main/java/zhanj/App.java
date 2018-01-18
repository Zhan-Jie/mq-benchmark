package zhanj;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

/**
 * Hello world!
 *
 */
public class App  {
    private static final int MESSAGES_PER_THREAD = 200000;

    public static void main( String[] args ) throws IOException, TimeoutException, InterruptedException {
        ObjectMapper om = new ObjectMapper();
        URL url = App.class.getClassLoader().getResource("config.json");
        JsonNode node = om.readTree(url);
        JsonNode connInfo = node.get("connection");

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(connInfo.get("host").asText());
        factory.setPort(connInfo.get("port").asInt(5672));
        factory.setUsername(connInfo.get("username").asText());
        factory.setPassword(connInfo.get("password").asText());

        ArrayNode cases = (ArrayNode) node.get("test_cases");
        for (JsonNode cas : cases) {
            int ex = cas.get("exchanges_number").asInt();
            int qPerEx = cas.get("queues_per_exchange").asInt();
            int pPerEx = cas.get("producers_per_exchange").asInt();
            boolean autoAck = cas.get("auto_ack").asBoolean();
            boolean durable = cas.get("durable").asBoolean();
            boolean mirrorQ = cas.get("mirror_queues").asBoolean();
            String confirmOrTx = cas.get("confirm_or_tx").asText();

            CountDownLatch latch = new CountDownLatch(1);
            TestCase tc = new TestCase(ex, qPerEx, pPerEx, factory.newConnection(), latch);
            tc.setAutoAck(autoAck);
            tc.setDurable(durable);
            tc.setMirrorQueues(mirrorQ);
            tc.setConfirmOrTx(confirmOrTx);
            tc.initialize();
            tc.start();

            latch.await();
        }
    }
}
