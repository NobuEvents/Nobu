package com.nobu.connect.sap.model;

import java.util.Map;

/**
 * Unified model for CDC records across all protocols (JDBC, REST, Data Services).
 * This provides a common representation of change data capture records.
 */
public class ChangeRecord {
    private String transactionId;
    private String tableName;
    private ChangeType changeType;
    private Map<String, Object> beforeValues;
    private Map<String, Object> afterValues;
    private Long timestamp;
    private String operationId;
    private Long offset; // For tracking position in CDC log

    public ChangeRecord() {
    }

    public ChangeRecord(String transactionId, String tableName, ChangeType changeType,
                       Map<String, Object> beforeValues, Map<String, Object> afterValues,
                       Long timestamp, String operationId, Long offset) {
        this.transactionId = transactionId;
        this.tableName = tableName;
        this.changeType = changeType;
        this.beforeValues = beforeValues;
        this.afterValues = afterValues;
        this.timestamp = timestamp;
        this.operationId = operationId;
        this.offset = offset;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public ChangeType getChangeType() {
        return changeType;
    }

    public void setChangeType(ChangeType changeType) {
        this.changeType = changeType;
    }

    public Map<String, Object> getBeforeValues() {
        return beforeValues;
    }

    public void setBeforeValues(Map<String, Object> beforeValues) {
        this.beforeValues = beforeValues;
    }

    public Map<String, Object> getAfterValues() {
        return afterValues;
    }

    public void setAfterValues(Map<String, Object> afterValues) {
        this.afterValues = afterValues;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getOperationId() {
        return operationId;
    }

    public void setOperationId(String operationId) {
        this.operationId = operationId;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    @Override
    public String toString() {
        return "ChangeRecord{" +
                "transactionId='" + transactionId + '\'' +
                ", tableName='" + tableName + '\'' +
                ", changeType=" + changeType +
                ", timestamp=" + timestamp +
                ", operationId='" + operationId + '\'' +
                ", offset=" + offset +
                '}';
    }
}
