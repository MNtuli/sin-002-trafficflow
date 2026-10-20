package co.wethinkcode.trafficflow;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class HubModelTest {

    @Test
    void testHubModelInstantiationAndGetters() {
        hub h = new hub("HUB_1", "Central", "traffic_light", true);

        assertThat(h.getId()).isEqualTo("HUB_1");
        assertThat(h.getDistrict()).isEqualTo("Central");
        assertThat(h.getSignalType()).isEqualTo("traffic_light");
        assertThat(h.getActiveflag()).isTrue();
        assertThat(h.toString()).isEqualTo("hub[id=HUB_1, district=Central, signalType=traffic_light, active=true]");
    }
}