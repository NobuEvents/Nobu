package com.nobu.schema;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
public class SchemaLoaderTest {

    @Inject
    public SchemaLoader loader;

    @Test
    public void testSchemaLoader() {

        assertTrue(loader.getSchemaMap().size() > 0);

        assertEquals("workflow", loader.getSchemaMap().get("srn:acme:workspace:add_widget:1.0.0").getDomain());

        String expected = "event == 'signup' && phone_number > 200 && user.phone > 2";
        assertEquals(expected, loader.getSchemaMap().get("srn:acme:growth:signup:1.0.0").getValidators().get("dlq").getQuery());

    }
}
