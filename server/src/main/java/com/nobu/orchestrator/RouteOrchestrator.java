package com.nobu.orchestrator;

import com.nobu.spi.connect.Connector;
import com.nobu.connect.ConnectorFactory;
import com.nobu.queue.EventQueue;
import com.nobu.queue.EventQueueFactory;
import com.nobu.queue.ValidationEventHandler;
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
public class RouteOrchestrator {

  private static final Logger LOG = Logger.getLogger(RouteOrchestrator.class);

  private RouteFactory routeFactory;

  @Inject
  EventQueueFactory eventQueueFactory;

  @Inject
  ConnectorFactory connectorFactory;

  @Inject
  ValidationEventHandler validationHandler;

  @Inject
  SourceConnectorManager sourceConnectorManager;

  @PostConstruct
  public void initialize() {
    LOG.info("Initializing Route Scheduler");
    var configFile = ConfigProvider.getConfig().getValue("router.config", String.class);
    LOG.info("Config file: " + configFile);
    routeFactory = RouteFactory.get(configFile);
    routeFactory.validateRouteConfig();
    startEventQueue(routeFactory);
    
    // Initialize and start source connectors
    sourceConnectorManager.initialize(routeFactory);
    sourceConnectorManager.start();
  }

  @PreDestroy
  public void terminate() {
    LOG.info("Terminating Route Scheduler");
    
    // Stop source connectors
    sourceConnectorManager.stop();
    
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
      if (eventQueueFactory.get(route.getId()) == null) {
        var queue = new EventQueue(route.getId());
        eventQueueFactory.put(route.getId(), queue);
        
        // Add validation handler as first handler (validates before routing)
        queue.addHandle(validationHandler);
      }

      LOG.info("Registering Connection Handler for route: " + route);
      var connectorHandle = getConnector(route, routeFactory.getConnectionMap());
      if (connectorHandle.isPresent()) {
        Objects.requireNonNull(eventQueueFactory.get(route.getId()))
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
