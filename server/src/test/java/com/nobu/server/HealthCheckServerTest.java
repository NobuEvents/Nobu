package com.nobu.server;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.is;

@QuarkusTest
public class HealthCheckServerTest {

    @Test
    public void testHealthCheck() {
        given()
                .when().get("/q/health/live")
                .then()
                .statusCode(200)
                .body("status", is("UP"),
                        "checks[0].name", is("Server is running"),
                        "checks[0].status", is("UP"));
    }
}
