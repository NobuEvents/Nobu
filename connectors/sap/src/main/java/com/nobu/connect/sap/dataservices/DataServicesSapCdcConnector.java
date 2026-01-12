package com.nobu.connect.sap.dataservices;

import com.nobu.connect.sap.SapCdcSourceConnector;
import com.nobu.connect.sap.protocol.ProtocolHandler;
import com.nobu.spi.source.SourceType;
import org.jboss.logging.Logger;

/**
 * SAP Data Services-based CDC source connector.
 * Integrates with SAP Data Services replication server for change data capture.
 */
public class DataServicesSapCdcConnector extends SapCdcSourceConnector {
    private static final Logger LOG = Logger.getLogger(DataServicesSapCdcConnector.class);

    @Override
    protected ProtocolHandler createProtocolHandler() {
        return new DataServicesProtocolHandler();
    }

    @Override
    public SourceType getSourceType() {
        return SourceType.SAP_DATA_SERVICES;
    }
}
