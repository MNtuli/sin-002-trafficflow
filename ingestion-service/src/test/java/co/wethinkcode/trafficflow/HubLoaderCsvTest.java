package co.wethinkcode.trafficflow;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class HubLoaderCsvTest {

    @Test
    @DisplayName("Should parse, normalize, and deduplicate actual CSV records")
    void testLoadActualCsv() throws Exception {
        List<Map<String, Object>> hubs = HubLoader.load("intersections-legacy.csv");

        // Out of 18 lines (minus header), INT-1005 is repeated once -> 17 unique records
        assertThat(hubs).hasSize(17);

        Map<String, Object> int1001 = findById(hubs, "INT-1001");
        assertThat(int1001).isNotNull();
        assertThat(int1001.get("district")).isEqualTo("Downtown");
        assertThat(int1001.get("signalType")).isEqualTo("4-way");
        assertThat(int1001.get("active")).isEqualTo(true);

        Map<String, Object> int1002 = findById(hubs, "INT-1002");
        assertThat(int1002).isNotNull();
        assertThat(int1002.get("district")).isEqualTo("Midtown");
        assertThat(int1002.get("signalType")).isEqualTo("pedestrian");
        assertThat(int1002.get("active")).isEqualTo(true);

        Map<String, Object> int1003 = findById(hubs, "INT-1003");
        assertThat(int1003.get("active")).isEqualTo(false);

        Map<String, Object> int1004 = findById(hubs, "INT-1004");
        assertThat(int1004.get("signalType")).isNull();
        assertThat(int1004.get("active")).isEqualTo(true);

        long count1005 = hubs.stream().filter(h -> "INT-1005".equals(h.get("id"))).count();
        assertThat(count1005).isEqualTo(1);

        Map<String, Object> int1007 = findById(hubs, "INT-1007");
        assertThat(int1007.get("signalType")).isNull();
        assertThat(int1007.get("active")).isEqualTo(true);

        Map<String, Object> int1013 = findById(hubs, "INT-1013");
        assertThat(int1013.get("signalType")).isNull();
        assertThat(int1013.get("active")).isEqualTo(false);


        Map<String, Object> int1015 = findById(hubs, "INT-1015");
        assertThat(int1015.get("district")).isNull();
        assertThat(int1015.get("signalType")).isEqualTo("4-way");
        assertThat(int1015.get("active")).isEqualTo(true);
    }

    private Map<String, Object> findById(List<Map<String, Object>> hubs, String id) {
        return hubs.stream()
                .filter(h -> id.equalsIgnoreCase((String) h.get("id")))
                .findFirst()
                .orElse(null);
    }
}