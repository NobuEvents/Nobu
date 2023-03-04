package com.nobu.scheduler;


import com.nobu.config.ConfigReader;
import com.nobu.config.YamlReader;
import com.nobu.connect.ConnectorFactory;
import com.nobu.exception.RouteConfigException;
import com.nobu.queue.DisruptorQueue;
import com.nobu.queue.DisruptorQueueFactory;
import com.nobu.route.Route;
import com.nobu.route.RouteFactory;
import com.nobu.schema.SchemaFactory;
import io.quarkus.runtime.Startup;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;


@Startup
@ApplicationScoped
public class RouteScheduler {

    private static final Logger LOG = Logger.getLogger(RouteScheduler.class);

    private RouteFactory routeFactory;
    private SchemaFactory schemaFactory;

    @Inject
    DisruptorQueueFactory disruptorQueueFactory;

    @Inject
    ConnectorFactory connectorFactory;

    @PostConstruct
    public void initialize() {
        LOG.info("Initializing Route Scheduler");
        var configFile = ConfigProvider.getConfig().getValue("router.config", String.class);
        this.routeFactory = RouteFactory.get(configFile);
        startDisruptor(this.routeFactory.getRoutes());
    }

    @PreDestroy
    public void terminate() {
        LOG.info("Terminating Route Scheduler");
        disruptorQueueFactory.terminate();
    }

    private void startDisruptor(Route[] routes) {
        for (var route : routes) {
            if (disruptorQueueFactory.get(route.getType()) == null) {
                var queue = new DisruptorQueue(route.getType());
                disruptorQueueFactory.put(route.getType(), queue);
            }
            LOG.info("Registering Connection Handler: " + connectorFactory.getConnector(route.getTarget()));
            Objects.requireNonNull(disruptorQueueFactory.get(route.getType()))
                    .addHandle(connectorFactory.getConnector(route.getTarget()));
        }
        disruptorQueueFactory.start();
        LOG.info("Disruptor started");
    }


    private Optional<SchemaFactory> getSchemaFactory(String path) {
        try {
            return Optional.of(YamlReader.getYaml(ConfigReader.getRouteConfig(path), SchemaFactory.class));
        } catch (IOException e) {
            LOG.error("Schema config not able to read", e);
            throw new RouteConfigException("Exception while reading schema config", e);
        }
    }

    public RouteFactory getRouteConfig() {
        return this.routeFactory;
    }
}
