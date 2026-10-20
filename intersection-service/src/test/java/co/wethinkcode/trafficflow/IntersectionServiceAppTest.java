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

class IntersectionServiceAppTest {

    private static Javalin mockIngestionServer;
    private static Javalin intersectionApp;
    private static String appBaseUrl;
    private static final HttpClient client = HttpClient.newHttpClient();

    @BeforeAll
    static void setUp() {
        // for Mock Ingestion Service endpoints
        mockIngestionServer = Javalin.create().start(0);
        int mockPort = mockIngestionServer.port();

        mockIngestionServer.get("/intersections/INT_VALID", ctx ->
                ctx.status(200).json("{\"id\":\"INT_VALID\",\"district\":\"Central\"}"));

        mockIngestionServer.get("/intersections/INT_INVALID", ctx ->
                ctx.status(404).result("not found: INT_INVALID"));

        mockIngestionServer.get("/intersections", ctx -> {
            String district = ctx.queryParam("district");
            if ("Central".equalsIgnoreCase(district)) {
                ctx.status(200).json("[{\"id\":\"INT_VALID\",\"district\":\"Central\"}]");
            } else {
                ctx.status(200).json("[]");
            }
        });

        //  Start IntersectionService pointing to mock ingestion service
        String mockIngestionUrl = "http://localhost:" + mockPort;
        intersectionApp = IntersectionServiceApp.createApp(mockIngestionUrl).start(0);
        appBaseUrl = "http://localhost:" + intersectionApp.port();
    }

    @AfterAll
    static void tearDown() {
        if (intersectionApp != null) {
            intersectionApp.stop();
        }
        if (mockIngestionServer != null) {
            mockIngestionServer.stop();
        }
    }

    @Test
    @DisplayName("GET /health should return 200 OK")
    void testHealthEndpoint() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(appBaseUrl + "/health"))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).isEqualTo("OK");
    }

    @Test
    @DisplayName("GET /intersections/{id}/validate returns 200 when ID exists upstream")
    void testValidateIntersectionSuccess() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(appBaseUrl + "/intersections/int_valid/validate"))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("\"valid\": true");
        assertThat(response.body()).contains("\"id\": \"INT_VALID\"");
    }

    @Test
    @DisplayName("GET /intersections/{id}/validate returns 404 when ID does not exist")
    void testValidateIntersectionNotFound() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(appBaseUrl + "/intersections/int_invalid/validate"))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(response.body()).contains("\"valid\": false");
    }

    @Test
    @DisplayName("GET /districts/{district}/validate returns 200 when district has intersections")
    void testValidateDistrictSuccess() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(appBaseUrl + "/districts/Central/validate"))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("\"valid\": true");
        assertThat(response.body()).contains("\"district\": \"Central\"");
    }

    @Test
    @DisplayName("GET /districts/{district}/validate returns 404 when district has no intersections")
    void testValidateDistrictNotFound() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(appBaseUrl + "/districts/NonExistentDistrict/validate"))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(response.body()).contains("\"valid\": false");
    }
}