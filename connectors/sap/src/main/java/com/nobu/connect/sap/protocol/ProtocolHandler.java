package com.nobu.connect.sap.protocol;

import com.nobu.connect.sap.model.ChangeRecord;

import java.util.List;
import java.util.Map;

/**
 * Abstract interface for protocol-specific operations.
 * Implementations handle JDBC, REST, or SAP Data Services protocols.
 */
public interface ProtocolHandler {
    
    /**
     * Poll for new changes from the source system.
     * 
     * @param lastOffset The last processed offset (for incremental polling)
     * @param batchSize Maximum number of records to return
     * @return List of change records
     */
    List<ChangeRecord> pollChanges(long lastOffset, int batchSize);
    
    /**
     * Perform a JOIN operation with a source table to enrich data.
     * 
     * @param tableName The name of the table to join with
     * @param joinKey The key field name to join on
     * @param joinValue The value to join with
     * @return Map of joined data (field name -> value)
     */
    Map<String, Object> performJoin(String tableName, String joinKey, Object joinValue);
    
    /**
     * Initialize the protocol handler with configuration.
     * 
     * @param config Configuration map
     */
    void initialize(Map<String, String> config);
    
    /**
     * Shutdown the protocol handler and clean up resources.
     */
    void shutdown();
    
    /**
     * Check if the protocol handler is healthy and can process requests.
     * 
     * @return true if healthy, false otherwise
     */
    boolean isHealthy();
}
