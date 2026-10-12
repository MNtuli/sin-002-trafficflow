package co.wethinkcode.trafficflow;

import com.opencsv.CSVReader;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class HubLoader {

    public static List<Map> load(String resourceName) throws Exception {
        //Map<String, hub> byId = new LinkedHashMap<>();
        List<Map> dataList = new ArrayList<>();

        try (InputStream is = Thread.currentThread()
                .getContextClassLoader()
                .getResourceAsStream(resourceName)) {

            if (is == null) {
                throw new IllegalArgumentException("Resource not found: " + resourceName);
            }

            try (CSVReader csvReader = new CSVReader(
                    new InputStreamReader(is, StandardCharsets.UTF_8))) {

                //skip first line
                csvReader.readNext();

                String[] nextRecord;

                // cleaning and grouping the data
                while ((nextRecord = csvReader.readNext()) != null) {
                    // skip if the is data missing
                    if (nextRecord.length < 4) continue;
                    Map<String, Object> dict = new LinkedHashMap<>();
                    String id = clean(nextRecord[0]).toUpperCase();
                    if (id.isEmpty()) continue;
                    dict.put("id", id);
                    dict.put("district", titleCase(nextRecord[1]));
                    dict.put("signalType", normalizeSignal(nextRecord[2]));
                    dict.put("active", parseFlag(nextRecord[3]));
                    dataList.add(dict);
                }
            }
        }
        return dataList;
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

    // Y/yes/true/1 -> true; everything else (N/no/false/0/unknown/blank) -> false
    private static boolean parseFlag(String s) {
        switch (clean(s).toLowerCase()) {
            case "y": case "yes": case "true": case "1": return true;
            default: return false;
        }
    }
}