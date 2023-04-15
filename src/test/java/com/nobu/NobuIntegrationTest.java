package com.nobu;

import com.nobu.event.NobuEvent;
import com.nobu.scheduler.RouteScheduler;
import io.quarkus.test.junit.QuarkusTest;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import javax.inject.Inject;
import javax.ws.rs.core.MediaType;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;


@QuarkusTest
@Tag("integration")
public class NobuIntegrationTest {


    KafkaContainer kafkaContainer;

    @Inject
    RouteScheduler routeScheduler;

    @BeforeEach
    public void setUp() {
        kafkaContainer = new KafkaContainer
                (DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
                .withReuse(false);
        kafkaContainer.start();
    }

    @Test
    public void testHelloEndpoint() {
        assertNotNull(kafkaContainer.getBootstrapServers());
        routeScheduler
                .getRouteConfig()
                .getConnectionMap()
                .get("front_kafka")
                .getConfig()
                .put("bootstrap.servers", kafkaContainer.getBootstrapServers());

        given()
                .contentType(MediaType.APPLICATION_JSON)
                .body(getInValidNobuEvent())
                .when().post("/event")
                .then()
                .statusCode(200)
                .body(is("ok"));

        given()
                .contentType(MediaType.APPLICATION_JSON)
                .body(getValidNobuEvent())
                .when().post("/event")
                .then()
                .statusCode(200)
                .body(is("ok"));

    }

    @NotNull
    private static NobuEvent getInValidNobuEvent() {
        byte[] invalidMessage = """
                {
                 "event":"event",
                 "widget_id": 300
                }
                """.getBytes();

        NobuEvent event = new NobuEvent();
        event.setType("signup");
        event.setSchema("add_widget");
        event.setMessage(invalidMessage);
        event.setTimestamp(1L);
        return event;
    }

    @NotNull
    private static NobuEvent getValidNobuEvent() {
        byte[] invalidMessage = """
                {
                 "event":"add_widget",
                 "widget_id": 300
                }
                """.getBytes();

        NobuEvent event = new NobuEvent();
        event.setType("signup");
        event.setSchema("add_widget");
        event.setMessage(invalidMessage);
        event.setTimestamp(1L);
        return event;
    }

    @AfterEach
    public void shutdown() {
        if (kafkaContainer != null && kafkaContainer.isRunning()) {
            kafkaContainer.stop();
        }
    }


}
