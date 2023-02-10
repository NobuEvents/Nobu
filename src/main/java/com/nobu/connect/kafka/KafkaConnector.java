package com.nobu.connect.kafka;

import com.nobu.connect.Connector;
import com.nobu.event.NobuEvent;
import org.jboss.logging.Logger;

public class KafkaConnector implements Connector {

    private static final Logger LOG = Logger.getLogger(KafkaConnector.class);

    @Override
    public void initialize() throws Exception {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public void onEvent(NobuEvent event, long sequence, boolean endOfBatch) throws Exception {
        LOG.info("KafkaConnector: " + event);
    }
}
