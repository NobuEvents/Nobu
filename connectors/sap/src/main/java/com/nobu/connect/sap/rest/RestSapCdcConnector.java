package com.nobu.connect.sap.rest;

import com.nobu.connect.sap.SapCdcSourceConnector;
import com.nobu.connect.sap.protocol.ProtocolHandler;
import com.nobu.spi.source.SourceType;
import org.jboss.logging.Logger;

/**
 * REST API-based SAP CDC source connector.
 * Connects to SAP OData/REST APIs for change data capture.
 */
public class RestSapCdcConnector extends SapCdcSourceConnector {
    private static final Logger LOG = Logger.getLogger(RestSapCdcConnector.class);

    @Override
    protected ProtocolHandler createProtocolHandler() {
        return new RestProtocolHandler();
    }

    @Override
    public SourceType getSourceType() {
        return SourceType.REST;
    }
}
