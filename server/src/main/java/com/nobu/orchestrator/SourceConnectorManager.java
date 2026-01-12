package com.nobu.orchestrator;

import com.nobu.route.RouteFactory;
import com.nobu.spi.source.SourceConnector;
import com.nobu.spi.source.SourceContext;
import org.jboss.logging.Logger;

import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages source connectors lifecycle.
 * Loads, initializes, starts, and manages source connectors from configuration.
 */
@ApplicationScoped
public class SourceConnectorManager {
    private static final Logger LOG = Logger.getLogger(SourceConnectorManager.class);
    
    private final Map<String, SourceConnector> sourceConnectors = new ConcurrentHashMap<>();
    private boolean initialized = false;

    /**
     * Initialize source connectors from route configuration.
     */
    public void initialize(RouteFactory routeFactory) {
        if (initialized) {
            return;
        }

        Map<String, RouteFactory.Source> sources = routeFactory.getSources();
        if (sources == null || sources.isEmpty()) {
            LOG.info("No source connectors configured");
            initialized = true;
            return;
        }

        LOG.info("Initializing " + sources.size() + " source connector(s)");

        for (Map.Entry<String, RouteFactory.Source> entry : sources.entrySet()) {
            String sourceName = entry.getKey();
            RouteFactory.Source sourceConfig = entry.getValue();

            try {
                SourceConnector connector = createSourceConnector(sourceConfig);
                
                SourceContext context = new SourceContext(
                    sourceConfig.getType(),
                    sourceConfig.getImpl(),
                    sourceConfig.getConfig(),
                    Map.of() // Route config not needed for sources
                );

                connector.initialize(sourceName, context);
                sourceConnectors.put(sourceName, connector);
                
                LOG.info("Source connector initialized: " + sourceName + " (type: " + sourceConfig.getType() + ")");

            } catch (Exception e) {
                LOG.error("Failed to initialize source connector: " + sourceName, e);
                // Continue with other connectors
            }
        }

        initialized = true;
    }

    /**
     * Start all source connectors.
     */
    public void start() {
        LOG.info("Starting " + sourceConnectors.size() + " source connector(s)");
        
        for (Map.Entry<String, SourceConnector> entry : sourceConnectors.entrySet()) {
            try {
                entry.getValue().start();
                LOG.info("Source connector started: " + entry.getKey());
            } catch (Exception e) {
                LOG.error("Failed to start source connector: " + entry.getKey(), e);
            }
        }
    }

    /**
     * Stop all source connectors.
     */
    public void stop() {
        LOG.info("Stopping " + sourceConnectors.size() + " source connector(s)");
        
        for (Map.Entry<String, SourceConnector> entry : sourceConnectors.entrySet()) {
            try {
                entry.getValue().stop();
            } catch (Exception e) {
                LOG.error("Failed to stop source connector: " + entry.getKey(), e);
            }
        }
    }

    /**
     * Create a source connector instance from configuration.
     */
    private SourceConnector createSourceConnector(RouteFactory.Source sourceConfig) {
        String implClass = sourceConfig.getImpl();
        
        if (implClass == null || implClass.isEmpty()) {
            throw new IllegalArgumentException("Source connector implementation class is required");
        }

        try {
            Class<?> clazz = Class.forName(implClass);
            if (!SourceConnector.class.isAssignableFrom(clazz)) {
                throw new IllegalArgumentException("Class " + implClass + " does not implement SourceConnector");
            }
            
            return (SourceConnector) clazz.getDeclaredConstructor().newInstance();
            
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Source connector class not found: " + implClass, e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to instantiate source connector: " + implClass, e);
        }
    }

    /**
     * Get a source connector by name.
     */
    public SourceConnector getSourceConnector(String name) {
        return sourceConnectors.get(name);
    }

    /**
     * Get all source connectors.
     */
    public Map<String, SourceConnector> getAllSourceConnectors() {
        return new HashMap<>(sourceConnectors);
    }

    @PreDestroy
    public void shutdown() {
        LOG.info("Shutting down source connectors");
        
        for (Map.Entry<String, SourceConnector> entry : sourceConnectors.entrySet()) {
            try {
                entry.getValue().shutdown();
            } catch (Exception e) {
                LOG.error("Error shutting down source connector: " + entry.getKey(), e);
            }
        }
        
        sourceConnectors.clear();
    }
}
