package co.wethinkcode.trafficflow;

import co.wethinkcode.trafficflow.mq.MqConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RoutingServiceApp {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final Map<String, Double> CONGESTION_MAP = new ConcurrentHashMap<>();
    public static final double BASE_TRAVEL_TIME_MINUTES = 5.0;

    public static Javalin createApp() {
        Javalin app = Javalin.create();

        app.get("/health", ctx -> ctx.result("OK"));

        // TODO (Provides estimated travel times based on congestion and intersection.)
        // Add domain endpoints for routing-service here.

        app.get("/routes/estimate", ctx -> {
            String intersectionId = ctx.queryParam("intersection");
            String from = ctx.queryParam("from");
            String to = ctx.queryParam("to");

            if (intersectionId == null && (from == null || to == null)) {
                ctx.status(400).contentType("application/json")
                        .result("{\"error\":\"Query parameter 'intersection' or both 'from' and 'to' are required.\"}");
                return;
            }

            double congestionFactor = 1.0;
            String targetId;

            if (intersectionId != null) {
                targetId = intersectionId.toUpperCase();
                congestionFactor = CONGESTION_MAP.getOrDefault(targetId, 1.0);
            } else {
                targetId = from.toUpperCase() + "->" + to.toUpperCase();
                double factorFrom = CONGESTION_MAP.getOrDefault(from.toUpperCase(), 1.0);
                double factorTo = CONGESTION_MAP.getOrDefault(to.toUpperCase(), 1.0);
                congestionFactor = Math.max(factorFrom, factorTo);
            }

            double estimatedTimeMinutes = BASE_TRAVEL_TIME_MINUTES * congestionFactor;

            ctx.contentType("application/json").result(
                    "{" +
                            "\"route\":\"" + targetId + "\"," +
                            "\"baseTimeMinutes\":" + BASE_TRAVEL_TIME_MINUTES + "," +
                            "\"congestionMultiplier\":" + congestionFactor + "," +
                            "\"estimatedTimeMinutes\":" + estimatedTimeMinutes +
                            "}"
            );
        });

        app.get("/routes/congestion", ctx -> ctx.json(CONGESTION_MAP));

        return app;
    }

    public static void startTopicSubscriber(String brokerUrl) {
        new Thread(() -> {
            try {
                ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
                Connection connection = factory.createConnection();
                connection.start();

                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                Topic topic = session.createTopic(MqConfig.TOPIC);
                MessageConsumer consumer = session.createConsumer(topic);

                consumer.setMessageListener(message -> {
                    if (message instanceof TextMessage textMessage) {
                        try {
                            String text = textMessage.getText();
                            JsonNode json = OBJECT_MAPPER.readTree(text);

                            String id = null;
                            if (json.has("intersectionId")) {
                                id = json.get("intersectionId").asText().toUpperCase();
                            } else if (json.has("id")) {
                                id = json.get("id").asText().toUpperCase();
                            }

                            if (id != null) {
                                double multiplier = 1.0;
                                if (json.has("congestionFactor")) {
                                    multiplier = json.get("congestionFactor").asDouble(1.0);
                                } else if (json.has("level")) {
                                    String level = json.get("level").asText().toUpperCase();
                                    multiplier = switch (level) {
                                        case "LOW" -> 1.0;
                                        case "MODERATE", "MEDIUM" -> 1.5;
                                        case "HIGH", "HEAVY" -> 2.2;
                                        case "SEVERE", "JAMMED" -> 3.5;
                                        default -> json.get("level").asDouble(1.0);
                                    };
                                }
                                CONGESTION_MAP.put(id, multiplier);
                            }
                        } catch (Exception e) {
                            System.err.println("[ROUTING ERROR] Error parsing congestion message: " + e.getMessage());
                        }
                    }
                });
            } catch (Exception e) {
                System.err.println("[ROUTING ERROR] Failed to connect to ActiveMQ: " + e.getMessage());
            }
        }, "routing-topic-subscriber").start();
    }

    public static void main(String[] args) {
        createApp().start(7023);
        startTopicSubscriber(MqConfig.BROKER_URL);
    }
}