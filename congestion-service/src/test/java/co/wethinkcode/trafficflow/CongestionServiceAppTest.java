package co.wethinkcode.trafficflow;

import co.wethinkcode.trafficflow.mq.MqConfig;
import io.javalin.Javalin;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class CongestionServiceAppTest {

    private static final String EMBEDDED_BROKER_URL = "vm://localhost?broker.persistent=false";
    private static BrokerService broker;
    private static Javalin app;
    private static String baseUrl;
    private static final HttpClient client = HttpClient.newHttpClient();

    @BeforeAll
    static void setUpAll() throws Exception {
        // 1. Start an in-memory ActiveMQ broker
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.addConnector(EMBEDDED_BROKER_URL);
        broker.start();
        broker.waitUntilStarted();

        // 2. Start the Congestion Service Javalin app on a dynamic available port
        app = CongestionServiceApp.createApp(EMBEDDED_BROKER_URL).start(0);
        baseUrl = "http://localhost:" + app.port();
    }

    @AfterAll
    static void tearDownAll() throws Exception {
        if (app != null) {
            app.stop();
        }
        if (broker != null) {
            broker.stop();
        }
    }

    @Test
    @DisplayName("GET /health returns 200 OK")
    void testHealthEndpoint() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/health"))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).isEqualTo("OK");
    }

    @Test
    @DisplayName("GET /congestion returns current city congestion level")
    void testGetCongestionDefault() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/congestion"))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("\"cityCongestionLevel\"");
    }

    @Test
    @DisplayName("POST /congestion updates level and publishes message to ActiveMQ topic")
    void testPostCongestionValidAndPublishesMq() throws Exception {
        // Subscribe to the ActiveMQ topic to intercept the message
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(EMBEDDED_BROKER_URL);
        Connection conn = factory.createConnection();
        conn.start();
        Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic(MqConfig.TOPIC);
        MessageConsumer consumer = session.createConsumer(topic);

        CountDownLatch messageLatch = new CountDownLatch(1);
        AtomicReference<String> receivedPayload = new AtomicReference<>();

        consumer.setMessageListener(msg -> {
            if (msg instanceof TextMessage textMsg) {
                try {
                    receivedPayload.set(textMsg.getText());
                    messageLatch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        // Send POST to update congestion level to 5
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/congestion"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"level\": 5}"))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("\"cityCongestionLevel\":5");
        assertThat(response.body()).contains("\"status\":\"UPDATED\"");

        // Verify that ActiveMQ topic consumer received the event
        boolean received = messageLatch.await(3, TimeUnit.SECONDS);
        assertThat(received).isTrue();
        assertThat(receivedPayload.get()).contains("\"type\":\"CITY_UPDATE\"");
        assertThat(receivedPayload.get()).contains("\"level\":5");

        // Cleanup JMS subscriber
        consumer.close();
        session.close();
        conn.close();
    }

    @Test
    @DisplayName("POST /congestion rejects level below 0 with 400")
    void testPostCongestionBelowZero() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/congestion"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"level\": -1}"))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).contains("Congestion level must be between 0 and 8");
    }

    @Test
    @DisplayName("POST /congestion rejects level above 8 with 400")
    void testPostCongestionAboveEight() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/congestion"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"level\": 9}"))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).contains("Congestion level must be between 0 and 8");
    }

    @Test
    @DisplayName("POST /congestion returns 400 when 'level' property is missing")
    void testPostCongestionMissingField() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/congestion"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"value\": 3}"))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).contains("Missing 'level' field");
    }
}