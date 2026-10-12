package co.wethinkcode.trafficflow;

import com.opencsv.CSVReader;
import io.javalin.Javalin;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class IngestionServiceApp {

    public static void main(String[] args) throws Exception {
        Javalin app = Javalin.create().start(7020);

        app.get("/health", ctx -> ctx.result("OK"));

        // TODO: read and clean src/main/resources/intersections-legacy.csv (intersections, districts, signal types data —
        // trim whitespace, fix casing, normalize dates/booleans) and expose the
        // cleaned records here for the other services to consume.
//        ("intersections-legacy.csv");
        List<Map> hubs = HubLoader.load("intersections-legacy.csv");
        for (Map cell : hubs){
            System.out.println(cell);
        }
        // get all cleaned data
        app.get("/intersections", ctx ->{
            String district = ctx.queryParam("district");
            if (district == null){
                ctx.json(hubs);
            }else {
                ctx.json(hubs.stream().filter(h ->district.equalsIgnoreCase((String) h.get("district"))).toList());
            }
        });
        app.get("/intersections/{id}",ctx ->{
            String id = ctx.pathParam("id").toUpperCase();
            hubs.stream().filter(h -> id.equalsIgnoreCase((String) h.get("id"))).findFirst().ifPresentOrElse(ctx::json,
                    () ->ctx.status(404).result("not found" + id));
        });
    }
}
