package co.wethinkcode.trafficflow;

import co.wethinkcode.trafficflow.mq.MqConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

public class CongestionServiceApp {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AtomicInteger cityCongestionLevel = new AtomicInteger(0);
    private static String brokerUrl = MqConfig.BROKER_URL;

    public static Javalin createApp(String activeMqBrokerUrl) {
        brokerUrl = activeMqBrokerUrl;
        Javalin app = Javalin.create();

        app.get("/health", ctx -> ctx.result("OK"));

        // TODO (Tracks the city-wide Congestion Level (0-8).)
        // Add domain endpoints for congestion-service here.
        // GET current city-wide congestion level (0-8)

        app.get("/congestion", ctx -> {
            ctx.contentType("application/json").result(
                    "{cityCongestionLevel:" + cityCongestionLevel.get() + "}"
            );
        });

        // POST update to city-wide congestion level (0-8)
        app.post("/congestion", ctx -> {
            try {
                JsonNode body = MAPPER.readTree(ctx.body());
                if (!body.has("level")) {
                    ctx.status(400).result("Missing 'level' field");
                    return;
                }

                int level = body.get("level").asInt();
                if (level < 0 || level > 8) {
                    ctx.status(400).result("Congestion level must be between 0 and 8");
                    return;
                }

                cityCongestionLevel.set(level);

                // Publish update event to ActiveMQ topic
                String payload = String.format(
                        "{type:CITY_UPDATE,level:%d,timestamp:%s}",
                        level, Instant.now()
                );
                publishCongestionUpdate(payload);

                ctx.status(200).contentType("application/json").result(
                        "{cityCongestionLevel:" + level + ",status:UPDATED}"
                );
            } catch (Exception e) {
                ctx.status(400).result("Invalid request body: " + e.getMessage());
            }
        });

        return app;
    }

    public static Javalin createApp() {
        return createApp(MqConfig.BROKER_URL);
    }

    public static void main(String[] args) {
        createApp().start(7022);

        // TODO (Tracks the city-wide Congestion Level (0-8).)
        // Add domain endpoints for congestion-service here.
    }

    private static void publishCongestionUpdate(String payload) {
        new Thread(() -> {
            Connection connection = null;
            Session session = null;
            MessageProducer producer = null;
            try {
                ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
                connection = factory.createConnection();
                connection.start();

                session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                Topic topic = session.createTopic(MqConfig.TOPIC);
                producer = session.createProducer(topic);
                producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

                TextMessage message = session.createTextMessage(payload);
                producer.send(message);
            } catch (Exception e) {
                System.err.println("[CONGESTION SERVICE] Failed to publish topic message: " + e.getMessage());
            } finally {
                try {
                    if (producer != null) producer.close();
                    if (session != null) session.close();
                    if (connection != null) connection.close();
                } catch (Exception ignored) {
                }
            }
        }, "congestion-publisher").start();
    }
}
// MQ TODO: publishes to ActiveMQ topic MqConfig.TOPIC at MqConfig.BROKER_URL (see co.wethinkcode.trafficflow.mq.MqConfig)