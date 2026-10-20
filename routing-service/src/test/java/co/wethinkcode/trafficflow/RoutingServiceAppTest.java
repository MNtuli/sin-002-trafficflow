package co.wethinkcode.trafficflow;

import co.wethinkcode.trafficflow.mq.MqConfig;
import io.javalin.Javalin;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.assertj.core.api.Assertions.assertThat;

class RoutingServiceAppTest {

    private static final String EMBEDDED_BROKER_URL = "vm://localhost?broker.persistent=false";
    private static BrokerService broker;
    private static Javalin app;
    private static String baseUrl;
    private static final HttpClient client = HttpClient.newHttpClient();

    @BeforeAll
    static void setUpAll() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.addConnector(EMBEDDED_BROKER_URL);
        broker.start();
        broker.waitUntilStarted();

        app = RoutingServiceApp.createApp().start(0);
        baseUrl = "http://localhost:" + app.port();

        RoutingServiceApp.startTopicSubscriber(EMBEDDED_BROKER_URL);
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

    @BeforeEach
    void clearState() {
        RoutingServiceApp.CONGESTION_MAP.clear();
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
    @DisplayName("GET /routes/estimate returns 400 when required parameters are missing")
    void testEstimateMissingParams() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/routes/estimate"))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).contains("error");
    }

    @Test
    @DisplayName("GET /routes/estimate returns default 1.0 multiplier when no congestion data exists")
    void testEstimateDefaultCongestion() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/routes/estimate?intersection=INT_10"))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("\"congestionMultiplier\":1.0");
        assertThat(response.body()).contains("\"estimatedTimeMinutes\":5.0");
    }

    @Test
    @DisplayName("Topic subscriber updates congestion map and impacts route estimates")
    void testActiveMqTopicUpdatesCongestion() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(EMBEDDED_BROKER_URL);
        try (Connection conn = factory.createConnection();
             Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            conn.start();
            Topic topic = session.createTopic(MqConfig.TOPIC);
            MessageProducer producer = session.createProducer(topic);

            TextMessage message = session.createTextMessage(
                    "{\"intersectionId\":\"INT_A\",\"congestionFactor\":2.0}"
            );
            producer.send(message);
        }

        Thread.sleep(600);

        HttpRequest congestionReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/routes/congestion"))
                .GET()
                .build();
        HttpResponse<String> congestionResp = client.send(congestionReq, HttpResponse.BodyHandlers.ofString());
        assertThat(congestionResp.statusCode()).isEqualTo(200);
        assertThat(congestionResp.body()).contains("\"INT_A\":2.0");

        HttpRequest estimateReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/routes/estimate?intersection=INT_A"))
                .GET()
                .build();
        HttpResponse<String> estimateResp = client.send(estimateReq, HttpResponse.BodyHandlers.ofString());

        assertThat(estimateResp.statusCode()).isEqualTo(200);
        assertThat(estimateResp.body()).contains("\"congestionMultiplier\":2.0");
        assertThat(estimateResp.body()).contains("\"estimatedTimeMinutes\":10.0");
    }

    @Test
    @DisplayName("GET /routes/estimate with from/to applies the higher congestion factor between endpoints")
    void testEstimateFromToRoute() throws Exception {
        RoutingServiceApp.CONGESTION_MAP.put("INT_X", 1.2);
        RoutingServiceApp.CONGESTION_MAP.put("INT_Y", 2.5);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/routes/estimate?from=int_x&to=int_y"))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("\"route\":\"INT_X->INT_Y\"");
        assertThat(response.body()).contains("\"congestionMultiplier\":2.5");
        assertThat(response.body()).contains("\"estimatedTimeMinutes\":12.5");
    }
}