package com.nobu.connect.sap.model;

/**
 * Enumeration of change types in CDC records.
 */
public enum ChangeType {
    /**
     * Insert operation
     */
    INSERT,
    
    /**
     * Update operation
     */
    UPDATE,
    
    /**
     * Delete operation
     */
    DELETE
}
