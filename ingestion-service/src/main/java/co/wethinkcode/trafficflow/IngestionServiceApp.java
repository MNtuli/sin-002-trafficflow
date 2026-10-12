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
    }
}
