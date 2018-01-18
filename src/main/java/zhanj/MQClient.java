package zhanj;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

public class MQClient {
    private ConnectionFactory factory = new ConnectionFactory();

    public MQClient(String uri) throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
        factory.setUri(uri);
    }

    protected Connection newConnection() {
        try {
            return factory.newConnection();
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
        return null;
    }
}
