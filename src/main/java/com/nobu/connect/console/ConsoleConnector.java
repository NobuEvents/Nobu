package com.nobu.connect.console;

import com.nobu.connect.Connector;
import com.nobu.event.NobuEvent;
import com.nobu.route.RouteFactory;
import org.jboss.logging.Logger;

import java.util.Map;

public class ConsoleConnector implements Connector {

    private static final Logger LOG = Logger.getLogger(ConsoleConnector.class);

    @Override
    public void initialize(String target, RouteFactory.Connection connection, Map<String, String> routeConfig) {

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
