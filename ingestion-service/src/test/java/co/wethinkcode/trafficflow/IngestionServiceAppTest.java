package co.wethinkcode.trafficflow;

import io.javalin.Javalin;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.assertj.core.api.Assertions.assertThat;

class IngestionServiceAppTest {

    private static Javalin app;
    private static String baseUrl;
    private static final HttpClient client = HttpClient.newHttpClient();

    @BeforeAll
    static void setUp() throws Exception {
        // Start on random available port (0) to prevent port conflicts
        app = IngestionServiceApp.createApp().start(0);
        baseUrl = "http://localhost:" + app.port();
    }

    @AfterAll
    static void tearDown() {
        if (app != null) {
            app.stop();
        }
    }

    @Test
    @DisplayName("GET /health returns 200 and OK")
    void testHealthEndpoint() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/health"))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).isEqualTo("OK");
    }

    @Test
    @DisplayName("GET /intersections returns 200 and JSON array")
    void testGetAllIntersections() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/intersections"))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.headers().firstValue("content-type"))
                .hasValueSatisfying(ct -> assertThat(ct).contains("application/json"));
        assertThat(response.body()).startsWith("[").endsWith("]");
    }

    @Test
    @DisplayName("GET /intersections?district=... filters results case-insensitively")
    void testFilterByDistrict() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/intersections?district=central"))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).doesNotContain("\"district\":\"South\"");
    }

    @Test
    @DisplayName("GET /intersections/{id} returns 404 when ID does not exist")
    void testGetIntersectionNotFound() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/intersections/NON_EXISTENT_ID"))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(response.body()).contains("not foundNON_EXISTENT_ID");
    }
}