package com.nobu.spi.source;

/**
 * Interface for source connectors that poll external systems and inject events into Nobu.
 * 
 * Source connectors differ from sink connectors:
 * - Source connectors PRODUCE events (inject into EventQueue)
 * - Sink connectors CONSUME events (from EventQueue)
 * 
 * Source connectors typically run in background threads/schedulers and need lifecycle management.
 */
public interface SourceConnector {
    
    /**
     * Initialize the source connector with the given context.
     * 
     * @param sourceName The name of the source (from configuration)
     * @param context The source context containing configuration and routing information
     */
    void initialize(String sourceName, SourceContext context);
    
    /**
     * Start the source connector.
     * This should begin polling/subscribing to the external system.
     */
    void start();
    
    /**
     * Stop the source connector temporarily.
     * The connector can be restarted later.
     */
    void stop();
    
    /**
     * Shutdown the source connector permanently.
     * Clean up resources and connections.
     */
    void shutdown();
    
    /**
     * Get the source type of this connector.
     * 
     * @return The source type (JDBC, REST, or SAP_DATA_SERVICES)
     */
    SourceType getSourceType();
}
