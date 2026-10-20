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
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class IntersectionWatchdogAppTest {

    private static final String EMBEDDED_BROKER_URL = "vm://localhost?broker.persistent=false";
    private static BrokerService broker;
    private static Javalin app;
    private static String baseUrl;
    private static final HttpClient client = HttpClient.newHttpClient();

    @BeforeAll
    static void setUp() throws Exception {
        // Start embedded in-memory ActiveMQ broker
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.addConnector(EMBEDDED_BROKER_URL);
        broker.start();
        broker.waitUntilStarted();

        // Start Javalin app on random available port
        app = IntersectionWatchdogApp.createApp().start(0);
        baseUrl = "http://localhost:" + app.port();

        // Connect consumers to embedded broker
        IntersectionWatchdogApp.startActiveMqConsumers(EMBEDDED_BROKER_URL);
    }

    @AfterAll
    static void tearDown() throws Exception {
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
    @DisplayName("GET /status reflects incoming heartbeat messages over ActiveMQ")
    void testHeartbeatUpdatesStatus() throws Exception {
        // Send a heartbeat message to the embedded broker
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(EMBEDDED_BROKER_URL);
        try (Connection conn = factory.createConnection();
             Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            conn.start();
            Destination queue = session.createQueue(MqConfig.HEARTBEAT_QUEUE);
            MessageProducer producer = session.createProducer(queue);

            TextMessage msg = session.createTextMessage("{\"service\":\"intersection-service\",\"status\":\"ALIVE\"}");
            producer.send(msg);
        }

        // Allow consumer thread to process message
        Thread.sleep(800);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/status"))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("\"intersectionServiceHealthy\":true");
        assertThat(response.body()).contains("\"alertActive\":false");
        assertThat(IntersectionWatchdogApp.lastHeartbeat.get()).isNotNull();
    }

    @Test
    @DisplayName("Dead-letter queue message triggers alert")
    void testDeadLetterTriggersAlert() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(EMBEDDED_BROKER_URL);
        try (Connection conn = factory.createConnection();
             Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            conn.start();
            Destination dlq = session.createQueue(IntersectionWatchdogApp.DLQ_NAME);
            MessageProducer producer = session.createProducer(dlq);

            TextMessage dlqMessage = session.createTextMessage("Failed route validation job payload");
            producer.send(dlqMessage);
        }

        Thread.sleep(800);

        assertThat(IntersectionWatchdogApp.serviceAlertTriggered.get()).isTrue();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/status"))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.body()).contains("\"alertActive\":true");
    }
}