package com.nobu.spi.source;

/**
 * Enumeration of supported source connector types.
 */
public enum SourceType {
    /**
     * JDBC-based source connector (direct database connection)
     */
    JDBC,
    
    /**
     * REST API-based source connector (OData/REST APIs)
     */
    REST,
    
    /**
     * SAP Data Services-based source connector
     */
    SAP_DATA_SERVICES
}
