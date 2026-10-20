package co.wethinkcode.trafficflow;

import co.wethinkcode.trafficflow.mq.MqConfig;
import io.javalin.Javalin;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class IntersectionWatchdogApp {

    public static final String DLQ_NAME = "ActiveMQ.DLQ";
    public static final long HEARTBEAT_TIMEOUT_SECONDS = 15;

    public static final AtomicReference<Instant> lastHeartbeat = new AtomicReference<>(null);
    public static final AtomicBoolean serviceAlertTriggered = new AtomicBoolean(false);
    private static final ScheduledExecutorService SCHEDULER = Executors.newSingleThreadScheduledExecutor();

    public static void main(String[] args) {
        Javalin app = createApp().start(7024);
        startActiveMqConsumers(MqConfig.BROKER_URL);
        startWatchdogTimer();
    }

    public static Javalin createApp() {
        Javalin app = Javalin.create();

        app.get("/health", ctx -> ctx.result("OK"));

        app.get("/status", ctx -> {
            Instant last = lastHeartbeat.get();
            boolean isAlive = last != null &&
                    Instant.now().minusSeconds(HEARTBEAT_TIMEOUT_SECONDS).isBefore(last);

            ctx.contentType("application/json").result(
                    "{" +
                            "\"intersectionServiceHealthy\":" + isAlive + "," +
                            "\"lastHeartbeat\":" + (last == null ? "null" : "\"" + last + "\"") + "," +
                            "\"alertActive\":" + serviceAlertTriggered.get() +
                            "}"
            );
        });

        return app;
    }

    public static void startActiveMqConsumers(String brokerUrl) {
        new Thread(() -> {
            try {
                ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
                Connection connection = factory.createConnection();
                connection.start();

                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                // Consume Heartbeat
                Destination heartbeatQueue = session.createQueue(MqConfig.HEARTBEAT_QUEUE);
                MessageConsumer heartbeatConsumer = session.createConsumer(heartbeatQueue);
                heartbeatConsumer.setMessageListener(message -> {
                    if (message instanceof TextMessage) {
                        lastHeartbeat.set(Instant.now());
                        serviceAlertTriggered.set(false);
                    }
                });

                // Consume DLQ
                Destination dlq = session.createQueue(DLQ_NAME);
                MessageConsumer dlqConsumer = session.createConsumer(dlq);
                dlqConsumer.setMessageListener(message -> {
                    try {
                        String payload = (message instanceof TextMessage tm) ? tm.getText() : message.toString();
                        cryForHelp("Dead-letter message detected in " + DLQ_NAME + ": " + payload);
                    } catch (JMSException e) {
                        System.err.println("[WATCHDOG ERROR] " + e.getMessage());
                    }
                });
            } catch (Exception e) {
                System.err.println("[WATCHDOG ERROR] Connection failed: " + e.getMessage());
            }
        }, "test-mq-consumer").start();
    }

    public static void startWatchdogTimer() {
        SCHEDULER.scheduleAtFixedRate(() -> {
            Instant last = lastHeartbeat.get();
            if (last == null) return;

            long elapsed = Instant.now().getEpochSecond() - last.getEpochSecond();
            if (elapsed > HEARTBEAT_TIMEOUT_SECONDS) {
                if (!serviceAlertTriggered.getAndSet(true)) {
                    cryForHelp("Intersection Service DOWN! No heartbeat for " + elapsed + " seconds.");
                }
            }
        }, 5, 3, TimeUnit.SECONDS);
    }

    public static void cryForHelp(String reason) {
        serviceAlertTriggered.set(true);
        System.err.println("[ALERT - CRY FOR HELP] " + reason);
    }
}