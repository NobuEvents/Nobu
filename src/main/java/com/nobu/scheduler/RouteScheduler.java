package com.nobu.scheduler;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.nobu.connect.ConnectorFactory;
import com.nobu.queue.DisruptorQueue;
import com.nobu.queue.DisruptorQueueFactory;
import com.nobu.route.Route;
import com.nobu.route.RouteConfig;
import io.quarkus.runtime.Startup;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.Optional;


@Startup
@ApplicationScoped
public class RouteScheduler {

    private static final Logger LOG = Logger.getLogger(RouteScheduler.class);

    private RouteConfig routeConfig;

    @Inject
    DisruptorQueueFactory disruptorQueueFactory;

    @Inject
    ConnectorFactory connectorFactory;

    @PostConstruct
    public void initialize() {
        LOG.info("Initializing Route Scheduler");
        var configFile = ConfigProvider.getConfig().getValue("router.config", String.class);
        this.routeConfig = buildRouteConfig(configFile).orElseThrow(RuntimeException::new);
        startDisruptor(this.routeConfig.getRoutes());
    }

    private void startDisruptor(Route[] routes) {
        for (var route : routes) {
            if (disruptorQueueFactory.get(route.getType()) == null) {
                var queue = new DisruptorQueue(route.getType());
                disruptorQueueFactory.put(route.getType(), queue);
            }
            LOG.info("Registering Connection Handler: " + connectorFactory.getConnector(route.getTarget()));
            disruptorQueueFactory.get(route.getType()).addHandle(connectorFactory.getConnector(route.getTarget()));
        }
        disruptorQueueFactory.start();
        LOG.info("Disruptor started");
    }

    private Optional<RouteConfig> buildRouteConfig(String configFile) {
        try {
            var mapper = new ObjectMapper(new YAMLFactory());
            mapper.findAndRegisterModules();
            return Optional.of(mapper.readValue(new File(configFile), RouteConfig.class));
        } catch (IOException e) {
            LOG.error("Route config not able to read", e);
        }
        return Optional.empty();
    }

    public RouteConfig getRouteConfig() {
        return this.routeConfig;
    }
}
