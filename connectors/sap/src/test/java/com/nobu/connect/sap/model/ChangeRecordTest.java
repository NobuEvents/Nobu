package com.nobu.connect.sap.model;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ChangeRecordTest {

    @Test
    public void testChangeRecordCreation() {
        ChangeRecord record = new ChangeRecord();
        assertNotNull(record);
    }

    @Test
    public void testChangeRecordWithAllFields() {
        ChangeRecord record = new ChangeRecord();
        record.setTransactionId("txn1");
        record.setTableName("test_table");
        record.setChangeType(ChangeType.INSERT);
        record.setOperationId("op1");
        record.setOffset(100L);
        record.setTimestamp(System.currentTimeMillis());
        
        Map<String, Object> beforeValues = new HashMap<>();
        beforeValues.put("id", "123");
        record.setBeforeValues(beforeValues);
        
        Map<String, Object> afterValues = new HashMap<>();
        afterValues.put("id", "456");
        record.setAfterValues(afterValues);

        assertEquals("txn1", record.getTransactionId());
        assertEquals("test_table", record.getTableName());
        assertEquals(ChangeType.INSERT, record.getChangeType());
        assertEquals("op1", record.getOperationId());
        assertEquals(100L, record.getOffset());
        assertNotNull(record.getTimestamp());
        assertEquals(beforeValues, record.getBeforeValues());
        assertEquals(afterValues, record.getAfterValues());
    }

    @Test
    public void testChangeRecordToString() {
        ChangeRecord record = new ChangeRecord();
        record.setTransactionId("txn1");
        record.setTableName("test_table");
        record.setChangeType(ChangeType.UPDATE);
        record.setOperationId("op1");
        record.setOffset(100L);

        String toString = record.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("txn1"));
        assertTrue(toString.contains("test_table"));
        assertTrue(toString.contains("UPDATE"));
    }

    @Test
    public void testChangeRecordConstructor() {
        Map<String, Object> beforeValues = new HashMap<>();
        beforeValues.put("id", "123");
        
        Map<String, Object> afterValues = new HashMap<>();
        afterValues.put("id", "456");
        
        ChangeRecord record = new ChangeRecord(
            "txn1",
            "test_table",
            ChangeType.INSERT,
            beforeValues,
            afterValues,
            System.currentTimeMillis(),
            "op1",
            100L
        );

        assertEquals("txn1", record.getTransactionId());
        assertEquals("test_table", record.getTableName());
        assertEquals(ChangeType.INSERT, record.getChangeType());
        assertEquals(beforeValues, record.getBeforeValues());
        assertEquals(afterValues, record.getAfterValues());
    }
}
