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

}