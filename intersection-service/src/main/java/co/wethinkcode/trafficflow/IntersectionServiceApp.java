package co.wethinkcode.trafficflow;

import co.wethinkcode.trafficflow.mq.MqConfig;
import io.javalin.Javalin;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class IntersectionServiceApp {

    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
    private static final ScheduledExecutorService SCHEDULER = Executors.newSingleThreadScheduledExecutor();

    public static void main(String[] args) {
        String ingestionUrl = System.getenv().getOrDefault("INGESTION_SERVICE_URL", "http://localhost:7020");
        createApp(ingestionUrl).start(7021);

        startHeartbeatPublisher();
    }

    public static Javalin createApp(String ingestionServiceUrl) {
        Javalin app = Javalin.create();

        app.get("/health", ctx -> ctx.result("OK"));

        // TODO (Validates intersection/district names (source of truth).)
        // Add domain endpoints for intersection-service here.

        // Validates whether an intersection ID exists
        app.get("/intersections/{id}/validate", ctx -> {
            String id = ctx.pathParam("id").toUpperCase();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(ingestionServiceUrl + "/intersections/" + id))
                    .GET()
                    .build();

            try {
                HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
                ctx.contentType("application/json");
                if (response.statusCode() == 200) {
                    ctx.status(200).result("{\"valid\":true,\"id\":\"" + id + "\"}");
                } else {
                    ctx.status(404).result("{\"valid\":false,\"id\":\"" + id + "\"}");
                }
            } catch (IOException | InterruptedException e) {
                ctx.status(502).result("Error querying ingestion-service: " + e.getMessage());
            }
        });

        // Validates whether a district name exists and has registered intersections
        app.get("/districts/{district}/validate", ctx -> {
            String district = ctx.pathParam("district");
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(ingestionServiceUrl + "/intersections?district=" + district))
                    .GET()
                    .build();

            try {
                HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
                ctx.contentType("application/json");
                if (response.statusCode() == 200 && !response.body().trim().equals("[]")) {
                    ctx.status(200).result("{\"valid\":true,\"district\":\"" + district + "\"}");
                } else {
                    ctx.status(404).result("{\"valid\":false,\"district\":\"" + district + "\"}");
                }
            } catch (IOException | InterruptedException e) {
                ctx.status(502).result("Error querying ingestion-service: " + e.getMessage());
            }
        });

        return app;
    }

    private static void startHeartbeatPublisher() {
        SCHEDULER.scheduleAtFixedRate(() -> {
            Connection connection = null;
            Session session = null;
            MessageProducer producer = null;
            try {
                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(MqConfig.BROKER_URL);
                connection = connectionFactory.createConnection();
                connection.start();

                session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                Destination destination = session.createQueue(MqConfig.HEARTBEAT_QUEUE);

                producer = session.createProducer(destination);
                String heartbeatPayload = "{\"service\":\"intersection-service\",\"status\":\"ALIVE\",\"timestamp\":\""
                        + Instant.now() + "\"}";

                TextMessage message = session.createTextMessage(heartbeatPayload);
                producer.send(message);
            } catch (Exception e) {
                System.err.println("Failed to send heartbeat to ActiveMQ: " + e.getMessage());
            } finally {
                try {
                    if (producer != null) producer.close();
                    if (session != null) session.close();
                    if (connection != null) connection.close();
                } catch (Exception ignored) {
                }
            }
        }, 0, 5, TimeUnit.SECONDS);
    }
}

// MQ TODO: publishes a periodic heartbeat to ActiveMQ queue MqConfig.HEARTBEAT_QUEUE at
// MqConfig.BROKER_URL (see co.wethinkcode.trafficflow.mq.MqConfig), consumed by intersection-watchdog.
