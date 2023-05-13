package com.nobu.cel;


import com.nobu.spi.event.NobuEvent;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;


import javax.inject.Inject;


import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
public class CelValidatorTest {

    @Inject
    CelValidator celValidator;

    @Test
    public void testValidateNobu() {

        byte[] message = """
                {
                 "event":"signup",
                 "client":"yahoo.com",
                 "phone_number":1234567890,
                 "user":{
                    "id":123,
                    "name":"John Doe",
                    "email":"mail@gmail.com",
                    "phone": 3
                 }
                }
                """.getBytes();

        NobuEvent event = new NobuEvent();
        event.setType("onboarding");
        event.setSchema("signup");
        event.setMessage(message);
        event.setTimestamp(1L);
        assertTrue(celValidator.test(event));
    }

}
