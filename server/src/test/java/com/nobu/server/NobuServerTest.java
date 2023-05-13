package com.nobu.server;

import com.nobu.spi.event.NobuEvent;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import javax.ws.rs.core.MediaType;
import java.io.IOException;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

@QuarkusTest
public class NobuServerTest {

    @Test
    public void testEventServer() throws IOException, InterruptedException {

        byte[] message = """
                {
                 "event":"signup",
                 "client":"yahoo.com",
                 "phone_number": 1234567890,
                 "user": {
                    "phone": 100
                 }
                }
                """.getBytes();

        NobuEvent event = new NobuEvent();
        event.setType("signup");
        event.setSchema("signup");

        event.setMessage(message);
        event.setTimestamp(1L);

        given()
                .contentType(MediaType.APPLICATION_JSON)
                .body(event)
                .when().post("/event")
                .then()
                .statusCode(200)
                .body(is("ok"));
    }

}
