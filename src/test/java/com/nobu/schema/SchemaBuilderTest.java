package com.nobu.schema;


import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class SchemaBuilderTest {

    @Test
    public void testSchemaFactory() {
        var schemaFactory = SchemaFactory.get("model/activity.schema.yaml");
        assertTrue(schemaFactory.isPresent());

        var schemaMap = schemaFactory.get();
        assertNotNull(schemaMap);
        assertNotNull(schemaMap.getSchemaMap());
        assertNotNull(schemaMap.getSchemaMap().get("signup"));

        var signupSchema = schemaMap.getSchemaMap().get("signup");
        assertEquals("entity", signupSchema.getType());
        assertEquals("core", signupSchema.getDomain());

        var test = signupSchema.getTest();
        assertEquals("event == 'signup' && phone_number > 10", test.getDlq()[0]);

        var fields = signupSchema.getFields();
        assertEquals("long", fields.get("account_id").getType());
        assertEquals("account", fields.get("account_id").getKey().getTarget());
    }
}
