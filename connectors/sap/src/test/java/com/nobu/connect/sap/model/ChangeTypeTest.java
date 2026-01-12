package com.nobu.connect.sap.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ChangeTypeTest {

    @Test
    public void testChangeTypeValues() {
        ChangeType[] values = ChangeType.values();
        assertEquals(3, values.length);
        
        assertTrue(contains(ChangeType.INSERT, values));
        assertTrue(contains(ChangeType.UPDATE, values));
        assertTrue(contains(ChangeType.DELETE, values));
    }

    @Test
    public void testChangeTypeValueOf() {
        assertEquals(ChangeType.INSERT, ChangeType.valueOf("INSERT"));
        assertEquals(ChangeType.UPDATE, ChangeType.valueOf("UPDATE"));
        assertEquals(ChangeType.DELETE, ChangeType.valueOf("DELETE"));
    }

    @Test
    public void testChangeTypeValueOfInvalid() {
        assertThrows(IllegalArgumentException.class, () -> {
            ChangeType.valueOf("INVALID");
        });
    }

    private boolean contains(ChangeType type, ChangeType[] array) {
        for (ChangeType t : array) {
            if (t == type) {
                return true;
            }
        }
        return false;
    }
}
