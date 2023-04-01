package com.nobu.schema;


import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class SchemaFactoryTest {

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

        SchemaFactory.QueryMeta test= signupSchema.getTest().getTests().get("dlq");
        assertEquals("event == 'signup' && phone_number > 200 && user.phone > 2", test.getQuery());

        var fields = signupSchema.getFields();
        assertEquals("long", fields.get("account_id").getType());
        assertEquals("account", fields.get("account_id").getKey().getTarget());
    }
}
