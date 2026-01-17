package com.nobu.connect.sap;

import com.nobu.connect.sap.model.ChangeRecord;
import com.nobu.connect.sap.model.ChangeType;
import com.nobu.connect.sap.model.TransactionState;
import com.nobu.connect.sap.model.TransactionStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class TransactionManagerReproductionTest {

    @TempDir
    Path tempDir;

    private TransactionManager transactionManager;

    @BeforeEach
    public void setUp() throws IOException {
        // Use a REAL storage path to enable LMDB and offloading
        // Tiny memory limit (100 bytes) to force immediate offloading
        transactionManager = new TransactionManager(
                100,
                60000,
                tempDir.toString());
        transactionManager.initialize();
    }

    @Test
    public void testOffloadedTransactionVisibility() throws IOException {
        String txId = "tx_offload_1";

        // 1. Create a large change that forces offload
        ChangeRecord change = createLargeChangeRecord(txId, "table1");
        transactionManager.addChange(change);

        // 2. Initial check - should be in progress
        // This might pass if offloading hasn't happened yet (it happens in addChange)
        // With limit 100 bytes, a large record will force offload immediately after add

        // 3. Force another check to ensure offload happened
        // The current implementation offloads if memory usage > limit
        // Let's verify we are actually offloaded
        TransactionState stateBound = transactionManager.getTransactionState(txId);
        // Note: getTransactionState MIGHT return it from disk if we fixed it,
        // or it might return null if the bug exists and we only check memory?
        // Actually getTransactionState checks memory THEN disk. So it might find it.
        assertNotNull(stateBound, "Should find transaction state even if offloaded");

        // 4. THE BUG: getInProgressTransactions only checks memory keySet
        List<TransactionState> inProgress = transactionManager.getInProgressTransactions();

        // This assertion is expected to FAIL before the fix
        boolean found = inProgress.stream()
                .anyMatch(t -> t.getTransactionId().equals(txId));

        assertTrue(found, "Offloaded transaction MUST be visible in inInProgressTransactions");

        // 5. Verify we can commit it
        List<ChangeRecord> committed = transactionManager.commitTransaction(txId);
        assertEquals(1, committed.size(), "Should be able to commit offloaded transaction");
    }

    private ChangeRecord createLargeChangeRecord(String transactionId, String tableName) {
        ChangeRecord record = new ChangeRecord();
        record.setTransactionId(transactionId);
        record.setTableName(tableName);
        record.setChangeType(ChangeType.INSERT);
        record.setOperationId("op1");
        record.setTimestamp(System.currentTimeMillis());

        Map<String, Object> afterValues = new HashMap<>();
        // 1KB payload
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1000; i++)
            sb.append("x");
        afterValues.put("data", sb.toString());
        record.setAfterValues(afterValues);

        return record;
    }
}
