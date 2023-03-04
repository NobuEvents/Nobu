package com.nobu.cel;


import com.nobu.event.NobuEvent;
import org.junit.jupiter.api.Test;
import org.projectnessie.cel.tools.ScriptException;

import java.io.IOException;

public class CelValidatorTest {

    @Test
    public void testValidateNobu() throws IOException, ScriptException {
        var celValidator = new CelValidator();
        byte[] message = """
                {
                 "event":"signup",
                 "client":"yahoo.com",
                 "user":{
                    "id":"123",
                    "name":"John Doe",
                    "email":"mail@gmail.com",
                    "phone": 2
                 }
                }
                """.getBytes();

        NobuEvent event = new NobuEvent();
        event.setType("signup");
        event.setMessage(message);
        event.setTimestamp(1L);
        System.out.println(celValidator.validateWithNobu(event));
    }

    @Test
    public void testSample() throws ScriptException {
        var celValidator = new CelValidator();
        celValidator.sample();
    }
}
