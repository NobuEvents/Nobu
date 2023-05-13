package com.nobu.spi.connect.logger;

import com.nobu.spi.connect.Connector;
import com.nobu.spi.connect.Context;
import com.nobu.spi.event.NobuEvent;
import org.jboss.logging.Logger;

public class LoggerConnector implements Connector {

    private static final Logger LOG = Logger.getLogger(LoggerConnector.class);

    @Override
    public void initialize(String target, Context context) {
        LOG.info("Initializing Logger Connector");
    }

    @Override
    public void shutdown() {
        LOG.info("Shutting down Logger Connector");
    }

    @Override
    public void onEvent(NobuEvent nobuEvent, long l, boolean b)
            throws Exception {
        LOG.info("Received event: " + nobuEvent);
    }
}
