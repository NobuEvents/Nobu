package com.nobu.connect;


import com.nobu.connect.console.ConsoleConnector;
import com.nobu.connect.kafka.KafkaConnector;
import com.nobu.connect.segment.SegmentConnector;

import javax.annotation.PostConstruct;
import javax.inject.Singleton;

import java.util.concurrent.ConcurrentHashMap;

@Singleton
public class ConnectorFactory {

    private ConcurrentHashMap<String, Connector> connectors;

    @PostConstruct
    public void init() {
        connectors = new ConcurrentHashMap<>();
        connectors.put("kafka", new KafkaConnector());
        connectors.put("segment", new SegmentConnector());
        connectors.put("console", new ConsoleConnector());
    }


    public Connector getConnector(String connectorName) {
        return connectors.get(connectorName);
    }

}
