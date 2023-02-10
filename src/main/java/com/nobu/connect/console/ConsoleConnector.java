package com.nobu.connect.console;

import com.nobu.connect.Connector;
import com.nobu.event.NobuEvent;
import org.jboss.logging.Logger;

public class ConsoleConnector implements Connector {

    private static final Logger LOG = Logger.getLogger(ConsoleConnector.class);

    @Override
    public void initialize() throws Exception {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public void onEvent(NobuEvent event, long sequence, boolean endOfBatch) throws Exception {
        LOG.info("ConsoleConnector: " + event);
        System.out.println(event);
    }
}
