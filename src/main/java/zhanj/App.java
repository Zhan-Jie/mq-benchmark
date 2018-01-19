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
    public static void main( String[] args ) throws IOException, TimeoutException, InterruptedException {
        ObjectMapper om = new ObjectMapper();
        URL url = App.class.getClassLoader().getResource("config.json");
        JsonNode node = om.readTree(url);
        JsonNode properties = node.get("properties");

        String[] hosts = properties.get("host").asText().split(",");
        int port = properties.get("port").asInt(5672);
        String username = properties.get("username").asText();
        String password = properties.get("password").asText();
        int ex = properties.get("exchanges_number").asInt();
        int qPerEx = properties.get("queues_per_exchange").asInt();
        int pPerEx = properties.get("producers_per_exchange").asInt();
        int msgPerP = 10000;

        if (args.length > 0) {
            for (String arg : args) {
                if (arg.startsWith("-host=")) {
                    hosts = arg.substring("-host=".length()).split(",");
                } else if (arg.startsWith("-port=")) {
                    port = Integer.parseInt(arg.substring("-port=".length()));
                } else if (arg.startsWith("-username=")) {
                    username = arg.substring("-username=".length());
                } else if (arg.startsWith("-password=")) {
                    password = arg.substring("-password=".length());
                } else if (arg.startsWith("-ex=")) {
                    ex = Integer.parseInt(arg.substring("-ex=".length()));
                } else if (arg.startsWith("-qPerEx=")) {
                    qPerEx = Integer.parseInt(arg.substring("-qPerEx=".length()));
                } else if (arg.startsWith("-pPerEx=")) {
                    pPerEx = Integer.parseInt(arg.substring("-pPerEx=".length()));
                } else if (arg.startsWith("-msgPerP=")) {
                    msgPerP = Integer.parseInt(arg.substring("-msgPerP=".length()));
                }
            }
        } else {
            showHelp();
        }

        ArrayNode cases = (ArrayNode) node.get("test_cases");
        for (JsonNode cas : cases) {
            boolean active = cas.get("active").asBoolean();
            if (!active) {
                continue;
            }

            List<Connection> connections = new ArrayList<>(hosts.length);
            for (String host : hosts) {
                ConnectionFactory factory = new ConnectionFactory();
                factory.setPort(port);
                factory.setUsername(username);
                factory.setPassword(password);
                factory.setHost(host);
                connections.add(factory.newConnection());
            }

            boolean autoAck = cas.get("auto_ack").asBoolean();
            boolean durable = cas.get("durable").asBoolean();
            boolean mirrorQ = cas.get("mirror_queues").asBoolean();
            String confirmOrTx = cas.get("confirm_or_tx").asText();

            CountDownLatch latch = new CountDownLatch(1);
            TestCase tc = new TestCase(ex, qPerEx, pPerEx, connections, msgPerP, latch);
            tc.setAutoAck(autoAck);
            tc.setDurable(durable);
            tc.setMirrorQueues(mirrorQ);
            tc.setConfirmOrTx(confirmOrTx);
            tc.initialize();
            tc.start();

            latch.await();

            tc.stop();
            System.out.println(tc.report());
        }
    }

    private static void showHelp () {
        System.out.println("help:\n");
        System.out.println("\tusage: java -jar mq-benchmark.jar -<option>=<value> ...\n");
        System.out.println("options:");
        System.out.println("\thost\t\tRabbitMQ host");
        System.out.println("\tport\t\tRabbitMQ client port");
        System.out.println("\tusername\t\tRabbitMQ username");
        System.out.println("\tpassword\t\tRabbitMQ user password");
        System.out.println("\tex\t\tnumber of exchanges");
        System.out.println("\tqPerEx\t\tnumber of related queues for per exchange");
        System.out.println("\tpPerEx\t\tnumber of related producers for per exchange");
        System.out.println("\tmsgPerP\t\tmessages of each producer will send");
    }
}
