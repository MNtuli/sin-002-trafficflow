package co.wethinkcode.trafficflow;

import io.javalin.Javalin;
import java.util.List;
import java.util.Map;

public class IngestionServiceApp {

    public static Javalin createApp() throws Exception {
        Javalin app = Javalin.create();

        app.get("/health", ctx -> ctx.result("OK"));
        // TODO: read and clean src/main/resources/intersections-legacy.csv (intersections, districts, signal types data —
        // trim whitespace, fix casing, normalize dates/booleans) and expose the
        // cleaned records here for the other services to consume.
        List<Map<String, Object>> hubs = HubLoader.load("intersections-legacy.csv");

        app.get("/intersections", ctx -> {
            String district = ctx.queryParam("district");
            if (district == null) {
                ctx.json(hubs);
            } else {
                ctx.json(hubs.stream()
                        .filter(h -> h.get("district") != null
                                && district.equalsIgnoreCase((String) h.get("district")))
                        .toList());
            }
        });

        app.get("/intersections/{id}", ctx -> {
            String id = ctx.pathParam("id").toUpperCase();
            hubs.stream()
                    .filter(h -> id.equalsIgnoreCase((String) h.get("id")))
                    .findFirst()
                    .ifPresentOrElse(
                            ctx::json,
                            () -> ctx.status(404).result("not found: " + id)
                    );
        });

        return app;
    }

    public static void main(String[] args) throws Exception {
        createApp().start(7020);
    }
}