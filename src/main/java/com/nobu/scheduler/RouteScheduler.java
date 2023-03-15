package com.nobu.scheduler;


import com.nobu.connect.Connector;
import com.nobu.connect.ConnectorFactory;
import com.nobu.queue.DisruptorQueue;
import com.nobu.queue.DisruptorQueueFactory;
import com.nobu.route.RouteFactory;
import io.quarkus.runtime.Startup;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import java.util.Map;
import java.util.Objects;


@Startup
@ApplicationScoped
public class RouteScheduler {

    private static final Logger LOG = Logger.getLogger(RouteScheduler.class);

    private RouteFactory routeFactory;

    @Inject
    DisruptorQueueFactory disruptorQueueFactory;

    @Inject
    ConnectorFactory connectorFactory;

    @PostConstruct
    public void initialize() {
        LOG.info("Initializing Route Scheduler");
        var configFile = ConfigProvider.getConfig().getValue("router.config", String.class);
        routeFactory = RouteFactory.get(configFile);
        routeFactory.validateRouteConfig();
        startDisruptor(routeFactory);
    }

    @PreDestroy
    public void terminate() {
        LOG.info("Terminating Route Scheduler");
        disruptorQueueFactory.terminate();
    }

    /***
     * Initialize the disruptor queue and register the connection handlers
     * If the queue is not found in the factory, create a new queue and register it
     * Register one disruptor queue per route type; one type can have multiple routes
     * @param routeFactory RouteFactory configuration file
     */
    private void startDisruptor(RouteFactory routeFactory) {
        for (var route : routeFactory.getRoutes()) {
            if (disruptorQueueFactory.get(route.getType()) == null) {
                var queue = new DisruptorQueue(route.getType());
                disruptorQueueFactory.put(route.getType(), queue);
            }

            LOG.info("Registering Connection Handler: " + getConnector(route, routeFactory.getConnectionMap()));
            Objects.requireNonNull(disruptorQueueFactory.get(route.getType()))
                    .addHandle(getConnector(route, routeFactory.getConnectionMap()));
        }
        disruptorQueueFactory.start();
        LOG.info("Disruptor started");
    }

    private Connector getConnector(RouteFactory.Route route, Map<String, RouteFactory.Connection> connectionMap) {
        return connectorFactory.getConnector(route.getTarget(),
                connectionMap.get(route.getTarget()), route.getConfig());
    }


    public RouteFactory getRouteConfig() {
        return this.routeFactory;
    }

}
