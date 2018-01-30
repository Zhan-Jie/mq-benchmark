package zhanj;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
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

        String mode = "producer";
        int group = 1;

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
                } else if (arg.startsWith("-mode=")) {
                    mode = arg.substring("-mode=".length());
                } else if (arg.startsWith("-group=")) {
                    group = Integer.parseInt(arg.substring("-group=".length()));
                }
            }
        } else {
            showHelp();
        }

        ArrayNode cases = (ArrayNode) node.get("test_cases");

        JsonNode cas = cases.get(group-1);

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

        if ("producer".equals(mode)) {
            ProducerTestGroup tc = new ProducerTestGroup(ex, qPerEx, pPerEx, durable, mirrorQ, confirmOrTx, msgPerP, connections);
            tc.initialize();
            tc.start();
            tc.stop();
            writeToFile("p-report.json", tc.report());
        } else if ("consumer".equals(mode)) {
            CountDownLatch latch = new CountDownLatch(1);
            ConsumerTestGroup tc = new ConsumerTestGroup(ex, qPerEx, autoAck, mirrorQ, ex*pPerEx*msgPerP, connections, latch);

            tc.initialize();
            tc.start();

            latch.await();
            tc.stop();
            writeToFile("c-report.json", tc.report());
        } else if ("prepare".equals(mode)) {
            Prepare pre = new Prepare(ex, qPerEx, durable, mirrorQ, connections);
            pre.prepare();
        } else {
            System.out.println("unknown option mode=" + mode);
        }
        for (Connection conn : connections) {
            if (conn.isOpen()) {
                conn.close();
                System.out.println("close connection to: " + conn.getAddress());
            }
        }
    }

    private static void writeToFile (String filename, String text) {
        try {
            FileWriter writer = new FileWriter(filename, false);
            writer.append(text);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
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
        System.out.println("\tmode\t\t'producer' or 'consumer'");
        System.out.println("\tgroup\t\t'1 ~ 6");
    }
}
