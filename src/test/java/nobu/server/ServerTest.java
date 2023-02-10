package nobu.server;

import com.nobu.event.NobuEvent;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import javax.ws.rs.core.MediaType;
import java.io.IOException;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

@QuarkusTest
public class ServerTest {

    @Test
    public void testEventServer() throws IOException, InterruptedException {

        byte[] message = """
                {
                 "event":"signup",
                 "client":"yahoo.com"
                }
                """.getBytes();

        NobuEvent event = new NobuEvent();
        event.setType("signup");
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
