package com.nobu.connect.segment;

import com.nobu.connect.Connector;
import com.nobu.event.NobuEvent;
import org.jboss.logging.Logger;

public class SegmentConnector implements Connector {

    private static final Logger LOG = Logger.getLogger(SegmentConnector.class);

    @Override
    public void initialize() throws Exception {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public void onEvent(NobuEvent event, long sequence, boolean endOfBatch) throws Exception {
        LOG.info("SegmentConnector: " + event);
    }
}
