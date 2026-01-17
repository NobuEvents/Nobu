package com.nobu.connect.sap.model;

/**
 * Status of a transaction in the CDC pipeline.
 */
public enum TransactionStatus {
    /**
     * Transaction has started but not yet committed
     */
    IN_PROGRESS,
    
    /**
     * Transaction has been committed
     */
    COMMITTED,
    
    /**
     * Transaction has been rolled back
     */
    ROLLED_BACK
}
