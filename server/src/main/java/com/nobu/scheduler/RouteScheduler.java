package com.nobu.scheduler;

import com.nobu.spi.connect.Connector;
import com.nobu.spi.connect.ConnectorFactory;
import com.nobu.queue.EventQueue;
import com.nobu.queue.EventQueueFactory;
import com.nobu.route.RouteFactory;
import io.quarkus.runtime.Startup;
import java.util.Optional;
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
  EventQueueFactory eventQueueFactory;

  @Inject
  ConnectorFactory connectorFactory;

  @PostConstruct
  public void initialize() {
    LOG.info("Initializing Route Scheduler");
    var configFile = ConfigProvider.getConfig().getValue("router.config", String.class);
    LOG.info("Config file: " + configFile);
    routeFactory = RouteFactory.get(configFile);
    routeFactory.validateRouteConfig();
    startEventQueue(routeFactory);
  }

  @PreDestroy
  public void terminate() {
    LOG.info("Terminating Route Scheduler");
    eventQueueFactory.terminate();
  }

  /***
   * Initialize the Event queue and register the connection handlers
   * If the queue is not found in the factory, create a new queue and register it
   * Register one Event queue per route type; one type can have multiple routes
   * @param routeFactory RouteFactory configuration file
   */
  private void startEventQueue(RouteFactory routeFactory) {
    for (var route : routeFactory.getRoutes()) {
      if (eventQueueFactory.get(route.getType()) == null) {
        var queue = new EventQueue(route.getType());
        eventQueueFactory.put(route.getType(), queue);
      }

      LOG.info("Registering Connection Handler for route: " + route);
      var connectorHandle = getConnector(route, routeFactory.getConnectionMap());
      if (connectorHandle.isPresent()) {
        Objects.requireNonNull(eventQueueFactory.get(route.getType()))
            .addHandle(connectorHandle.get());
      } else {
        LOG.error("Unable to register the connection handler for route: " + route);
      }
    }
    eventQueueFactory.start();
    LOG.info("Event Queue started");
  }

  private Optional<Connector> getConnector(RouteFactory.Route route,
      Map<String, RouteFactory.Connection> connectionMap) {
    return connectorFactory.getConnector(route.getTarget(),
        connectionMap.get(route.getTarget()), route.getConfig());
  }

  public RouteFactory getRouteConfig() {
    return this.routeFactory;
  }
}
