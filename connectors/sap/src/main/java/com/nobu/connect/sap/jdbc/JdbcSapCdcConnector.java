package com.nobu.connect.sap.jdbc;

import com.nobu.connect.sap.SapCdcSourceConnector;
import com.nobu.connect.sap.protocol.ProtocolHandler;
import com.nobu.spi.source.SourceType;
import org.jboss.logging.Logger;

/**
 * JDBC-based SAP CDC source connector.
 * Connects directly to SAP HANA database and polls CDC log tables.
 */
public class JdbcSapCdcConnector extends SapCdcSourceConnector {
    private static final Logger LOG = Logger.getLogger(JdbcSapCdcConnector.class);

    @Override
    protected ProtocolHandler createProtocolHandler() {
        return new JdbcProtocolHandler();
    }

    @Override
    public SourceType getSourceType() {
        return SourceType.JDBC;
    }
}
