package com.nobu.connect.sap.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class TransactionStatusTest {

    @Test
    public void testTransactionStatusValues() {
        TransactionStatus[] values = TransactionStatus.values();
        assertEquals(3, values.length);
        
        assertTrue(contains(TransactionStatus.IN_PROGRESS, values));
        assertTrue(contains(TransactionStatus.COMMITTED, values));
        assertTrue(contains(TransactionStatus.ROLLED_BACK, values));
    }

    @Test
    public void testTransactionStatusValueOf() {
        assertEquals(TransactionStatus.IN_PROGRESS, TransactionStatus.valueOf("IN_PROGRESS"));
        assertEquals(TransactionStatus.COMMITTED, TransactionStatus.valueOf("COMMITTED"));
        assertEquals(TransactionStatus.ROLLED_BACK, TransactionStatus.valueOf("ROLLED_BACK"));
    }

    @Test
    public void testTransactionStatusValueOfInvalid() {
        assertThrows(IllegalArgumentException.class, () -> {
            TransactionStatus.valueOf("INVALID");
        });
    }

    private boolean contains(TransactionStatus status, TransactionStatus[] array) {
        for (TransactionStatus s : array) {
            if (s == status) {
                return true;
            }
        }
        return false;
    }
}
