package com.nobu.connect.sap.model;

import com.nobu.connect.sap.model.ChangeRecord;
import com.nobu.connect.sap.model.ChangeType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class TransactionStateTest {

    private TransactionState transactionState;

    @BeforeEach
    public void setUp() {
        transactionState = new TransactionState("txn1");
    }

    @Test
    public void testTransactionStateCreation() {
        assertNotNull(transactionState);
        assertEquals("txn1", transactionState.getTransactionId());
        assertEquals(TransactionStatus.IN_PROGRESS, transactionState.getStatus());
        assertNotNull(transactionState.getChanges());
        assertTrue(transactionState.getChanges().isEmpty());
    }

    @Test
    public void testTransactionStateDefaultConstructor() {
        TransactionState state = new TransactionState();
        assertNotNull(state);
        assertEquals(TransactionStatus.IN_PROGRESS, state.getStatus());
        assertNotNull(state.getChanges());
    }

    @Test
    public void testAddChange() {
        ChangeRecord change = createTestChangeRecord();
        transactionState.addChange(change);

        List<ChangeRecord> changes = transactionState.getChanges();
        assertEquals(1, changes.size());
        assertEquals(change, changes.get(0));
    }

    @Test
    public void testAddMultipleChanges() {
        ChangeRecord change1 = createTestChangeRecord();
        ChangeRecord change2 = createTestChangeRecord();
        
        transactionState.addChange(change1);
        transactionState.addChange(change2);

        assertEquals(2, transactionState.getChanges().size());
    }

    @Test
    public void testSetStatus() {
        transactionState.setStatus(TransactionStatus.COMMITTED);
        assertEquals(TransactionStatus.COMMITTED, transactionState.getStatus());
        
        transactionState.setStatus(TransactionStatus.ROLLED_BACK);
        assertEquals(TransactionStatus.ROLLED_BACK, transactionState.getStatus());
    }

    @Test
    public void testIsInMemory() {
        assertTrue(transactionState.isInMemory());
        
        transactionState.setStoragePath("/path/to/storage");
        assertFalse(transactionState.isInMemory());
    }

    @Test
    public void testGetDuration() throws InterruptedException {
        long startTime = transactionState.getStartTime();
        Thread.sleep(10);
        long duration = transactionState.getDuration();
        
        assertTrue(duration >= 10);
        assertTrue(duration < 100); // Should be close to sleep time
    }

    @Test
    public void testUpdateMemorySize() {
        transactionState.updateMemorySize(1024L);
        assertEquals(1024L, transactionState.getMemorySize());
        
        transactionState.updateMemorySize(2048L);
        assertEquals(2048L, transactionState.getMemorySize());
    }

    @Test
    public void testToString() {
        transactionState.addChange(createTestChangeRecord());
        transactionState.setStatus(TransactionStatus.COMMITTED);
        
        String toString = transactionState.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("txn1"));
        assertTrue(toString.contains("COMMITTED"));
        assertTrue(toString.contains("changes=1"));
    }

    @Test
    public void testSetChanges() {
        List<ChangeRecord> newChanges = List.of(
            createTestChangeRecord(),
            createTestChangeRecord()
        );
        
        transactionState.setChanges(newChanges);
        assertEquals(2, transactionState.getChanges().size());
    }

    private ChangeRecord createTestChangeRecord() {
        ChangeRecord record = new ChangeRecord();
        record.setTransactionId("txn1");
        record.setTableName("test_table");
        record.setChangeType(ChangeType.INSERT);
        
        Map<String, Object> afterValues = new HashMap<>();
        afterValues.put("id", "123");
        record.setAfterValues(afterValues);
        
        return record;
    }
}
