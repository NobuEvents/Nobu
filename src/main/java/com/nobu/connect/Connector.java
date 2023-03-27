package com.nobu.connect;

import com.lmax.disruptor.EventHandler;
import com.nobu.event.NobuEvent;
import com.nobu.route.RouteFactory;

import java.util.Map;

public interface Connector extends EventHandler<NobuEvent> {
    void initialize(String target, RouteFactory.Connection connection, Map<String, String> routeConfig);

    void shutdown();
}
