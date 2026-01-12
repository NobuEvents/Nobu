package com.nobu.connect.sap.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents the state of a transaction in the CDC pipeline.
 * Tracks transaction lifecycle, associated changes, and memory/storage location.
 */
public class TransactionState {
    private String transactionId;
    private TransactionStatus status;
    private List<ChangeRecord> changes;
    private long startTime;
    private long memorySize; // Size in bytes
    private String storagePath; // null if in memory, path if offloaded to disk

    public TransactionState() {
        this.changes = new ArrayList<>();
        this.status = TransactionStatus.IN_PROGRESS;
        this.startTime = System.currentTimeMillis();
    }

    public TransactionState(String transactionId) {
        this();
        this.transactionId = transactionId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public TransactionStatus getStatus() {
        return status;
    }

    public void setStatus(TransactionStatus status) {
        this.status = status;
    }

    public List<ChangeRecord> getChanges() {
        return changes;
    }

    public void setChanges(List<ChangeRecord> changes) {
        this.changes = changes;
    }

    public void addChange(ChangeRecord change) {
        this.changes.add(change);
        // Memory size is now calculated externally by TransactionManager
        // This method is kept for backward compatibility but calculation is done there
    }
    
    /**
     * Update memory size estimate for this transaction.
     * Called by TransactionManager after calculating accurate size.
     */
    public void updateMemorySize(long size) {
        this.memorySize = size;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getMemorySize() {
        return memorySize;
    }

    public void setMemorySize(long memorySize) {
        this.memorySize = memorySize;
    }

    public String getStoragePath() {
        return storagePath;
    }

    public void setStoragePath(String storagePath) {
        this.storagePath = storagePath;
    }

    public boolean isInMemory() {
        return storagePath == null;
    }

    public long getDuration() {
        return System.currentTimeMillis() - startTime;
    }

    @Override
    public String toString() {
        return "TransactionState{" +
                "transactionId='" + transactionId + '\'' +
                ", status=" + status +
                ", changes=" + changes.size() +
                ", startTime=" + startTime +
                ", memorySize=" + memorySize +
                ", storagePath='" + storagePath + '\'' +
                '}';
    }
}
