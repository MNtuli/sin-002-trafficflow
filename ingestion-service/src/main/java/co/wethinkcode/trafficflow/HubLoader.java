package co.wethinkcode.trafficflow;

import com.opencsv.CSVReader;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class HubLoader {

    public static List<Map<String, Object>> load(String resourceName) throws Exception {
        // Use a LinkedHashMap to preserve insertion order while deduplicating by ID
        Map<String, Map<String, Object>> recordsById = new LinkedHashMap<>();

        try (InputStream is = Thread.currentThread()
                .getContextClassLoader()
                .getResourceAsStream(resourceName)) {

            if (is == null) {
                throw new IllegalArgumentException("Resource not found: " + resourceName);
            }

            try (CSVReader csvReader = new CSVReader(
                    new InputStreamReader(is, StandardCharsets.UTF_8))) {

                // Skip header row: intersection_id,District ,signal_type,active_flag
                csvReader.readNext();

                String[] nextRecord;

                while ((nextRecord = csvReader.readNext()) != null) {
                    if (nextRecord.length < 4) continue;

                    String id = clean(nextRecord[0]).toUpperCase();
                    if (id.isEmpty()) continue;

                    Map<String, Object> record = new LinkedHashMap<>();
                    record.put("id", id);
                    record.put("district", titleCase(nextRecord[1]));
                    record.put("signalType", normalizeSignal(nextRecord[2]));
                    record.put("active", parseFlag(nextRecord[3]));

                    recordsById.put(id, record);
                }
            }
        }
        return new ArrayList<>(recordsById.values());
    }

    private static String clean(String s) {
        return s == null ? "" : s.trim();
    }

    private static String titleCase(String s) {
        s = clean(s).toLowerCase();
        return s.isEmpty() ? null : Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }

    private static String normalizeSignal(String s) {
        s = clean(s).toLowerCase();
        return (s.isEmpty() || s.equals("unknown")) ? null : s;
    }

    // Y / yes / true / 1 -> true; everything else -> false
    private static boolean parseFlag(String s) {
        switch (clean(s).toLowerCase()) {
            case "y":
            case "yes":
            case "true":
            case "1":
                return true;
            default:
                return false;
        }
    }
}