package co.wethinkcode.trafficflow;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HubLoaderTest {

    @Test
    @DisplayName("Should throw IllegalArgumentException when resource does not exist")
    void testResourceNotFound() {
        assertThatThrownBy(() -> HubLoader.load("non-existent-file.csv"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Resource not found");
    }
    // Loads the actual legacy CSV from src/main/resources or src/test/resources
    // Verify ID is capitalized and trimmed
    // Verify active is a boolean
    @Test
    @DisplayName("Should load, clean, and normalize CSV records correctly")
    void testLoadAndCleanData() throws Exception {
        List<Map> records = HubLoader.load("intersections-legacy.csv");

        assertThat(records).isNotEmpty();

        Map<String, Object> first = records.get(0);
        assertThat(first).containsKeys("id", "district", "signalType", "active");

        assertThat(first.get("id").toString()).isEqualTo(first.get("id").toString().toUpperCase().trim());

        assertThat(first.get("active")).isInstanceOf(Boolean.class);
    }
}