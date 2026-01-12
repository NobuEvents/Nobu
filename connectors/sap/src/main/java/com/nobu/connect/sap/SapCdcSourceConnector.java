package com.nobu.connect.sap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nobu.connect.sap.model.ChangeRecord;
import com.nobu.connect.sap.model.TransactionState;
import com.nobu.connect.sap.protocol.ProtocolHandler;
import com.nobu.queue.EventQueueFactory;
import com.nobu.spi.source.SourceConnector;
import com.nobu.spi.source.SourceContext;
import com.nobu.spi.source.SourceType;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.jboss.logging.Logger;

import javax.enterprise.inject.Instance;
import javax.enterprise.inject.spi.CDI;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Abstract base class for SAP CDC source connectors.
 * Implements common CDC logic including transaction management, JOIN processing, and event injection.
 * 
 * Subclasses implement protocol-specific handlers (JDBC, REST, Data Services).
 */
public abstract class SapCdcSourceConnector implements SourceConnector {
    private static final Logger LOG = Logger.getLogger(SapCdcSourceConnector.class);
    
    protected EventQueueFactory eventQueueFactory;
    protected ObjectMapper objectMapper;
    protected MeterRegistry meterRegistry;
    protected String sourceName;
    protected SourceContext context;
    protected ProtocolHandler protocolHandler;
    protected TransactionManager transactionManager;
    protected JoinProcessor joinProcessor;
    protected SapEventInjector eventInjector;
    
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong lastOffset = new AtomicLong(0);
    private ScheduledExecutorService scheduler;
    
    // Configuration keys
    protected static final String POLL_INTERVAL_SECONDS = "poll.interval.seconds";
    protected static final String TRANSACTION_MEMORY_LIMIT_MB = "transaction.memory.limit.mb";
    protected static final String TRANSACTION_DURATION_LIMIT_SECONDS = "transaction.duration.limit.seconds";
    protected static final String STORAGE_PATH = "storage.path";
    protected static final String JOIN_ENABLED = "join.enabled";
    protected static final String BATCH_SIZE = "batch.size";
    protected static final String HOST = "host";
    protected static final String EVENT_BACKPRESSURE_TIMEOUT_MS = "event.backpressure.timeout.ms";
    
    // Default values
    private static final int DEFAULT_POLL_INTERVAL_SECONDS = 900; // 15 minutes
    private static final long DEFAULT_MEMORY_LIMIT_MB = 1024L;
    private static final long DEFAULT_DURATION_LIMIT_SECONDS = 60L;
    private static final int DEFAULT_BATCH_SIZE = 1000;
    private static final String DEFAULT_HOST = "sap-cdc";
    private static final long DEFAULT_BACKPRESSURE_TIMEOUT_MS = 1000L;

    // Metrics
    private Counter eventsProcessed;
    private Counter pollErrors;
    private Timer pollDuration;

    @Override
    public void initialize(String sourceName, SourceContext context) {
        this.sourceName = sourceName;
        this.context = context;
        
        try {
            // Lookup dependencies via CDI
            try {
                Instance<EventQueueFactory> eventQueueFactoryInstance = CDI.current().select(EventQueueFactory.class);
                if (eventQueueFactoryInstance.isResolvable()) {
                    this.eventQueueFactory = eventQueueFactoryInstance.get();
                } else {
                    throw new IllegalStateException("EventQueueFactory not available in CDI context");
                }
            } catch (Exception e) {
                LOG.warn("Could not lookup EventQueueFactory via CDI, connectors may not work properly", e);
            }
            
            try {
                Instance<ObjectMapper> objectMapperInstance = CDI.current().select(ObjectMapper.class);
                if (objectMapperInstance.isResolvable()) {
                    this.objectMapper = objectMapperInstance.get();
                } else {
                    this.objectMapper = new ObjectMapper();
                }
            } catch (Exception e) {
                LOG.warn("Could not lookup ObjectMapper via CDI, using new instance", e);
                this.objectMapper = new ObjectMapper();
            }

            try {
                Instance<MeterRegistry> registryInstance = CDI.current().select(MeterRegistry.class);
                if (registryInstance.isResolvable()) {
                    this.meterRegistry = registryInstance.get();
                } else {
                    // Create SimpleMeterRegistry as fallback if CDI doesn't provide one
                    this.meterRegistry = new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
                    LOG.info("Created SimpleMeterRegistry for SAP CDC metrics (JMX not available)");
                }
            } catch (Exception e) {
                LOG.warn("Could not lookup MeterRegistry via CDI, using SimpleMeterRegistry", e);
                // Fallback: create SimpleMeterRegistry
                this.meterRegistry = new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
            }
            
            // Initialize protocol handler (implemented by subclasses)
            this.protocolHandler = createProtocolHandler();
            // Securely log configuration (mask secrets if any)
            // Implementation of initialize inside handlers should handle secure logging
            this.protocolHandler.initialize(context.sourceConfig());
            
            // Initialize transaction manager
            long memoryLimit = getConfigLong(TRANSACTION_MEMORY_LIMIT_MB, DEFAULT_MEMORY_LIMIT_MB) * 1024 * 1024;
            long durationLimit = getConfigLong(TRANSACTION_DURATION_LIMIT_SECONDS, DEFAULT_DURATION_LIMIT_SECONDS) * 1000;
            String storagePath = context.sourceConfig().get(STORAGE_PATH);
            
            this.transactionManager = new TransactionManager(memoryLimit, durationLimit, storagePath);
            this.transactionManager.initialize();
            
            // Initialize join processor
            boolean joinEnabled = Boolean.parseBoolean(context.sourceConfig().getOrDefault(JOIN_ENABLED, "false"));
            Map<String, JoinProcessor.JoinConfig> joinConfigs = parseJoinConfigs();
            this.joinProcessor = new JoinProcessor(protocolHandler, joinConfigs, joinEnabled, meterRegistry);
            
            // Initialize event injector with configured timeout
            long backpressureTimeout = getConfigLong(EVENT_BACKPRESSURE_TIMEOUT_MS, DEFAULT_BACKPRESSURE_TIMEOUT_MS);
            this.eventInjector = new SapEventInjector(eventQueueFactory, objectMapper, backpressureTimeout);
            
            // Initialize Metrics
            if (this.meterRegistry != null) {
                this.eventsProcessed = Counter.builder("sap.cdc.events.processed")
                    .tag("source", sourceName)
                    .description("Number of SAP CDC events processed")
                    .register(meterRegistry);
                this.pollErrors = Counter.builder("sap.cdc.poll.errors")
                    .tag("source", sourceName)
                    .description("Number of polling errors")
                    .register(meterRegistry);
                this.pollDuration = Timer.builder("sap.cdc.poll.duration")
                    .tag("source", sourceName)
                    .description("Time taken for poll cycle")
                    .register(meterRegistry);
            }
            
            LOG.info("SAP CDC Source Connector initialized: " + sourceName);
            
        } catch (Exception e) {
            LOG.error("Failed to initialize SAP CDC Source Connector: " + sourceName, e);
            throw new RuntimeException("Failed to initialize SAP CDC connector", e);
        }
    }

    @Override
    public void start() {
        if (running.compareAndSet(false, true)) {
            LOG.info("Starting SAP CDC Source Connector: " + sourceName);
            
            // Start scheduled polling
            int pollIntervalSeconds = getConfigInt(POLL_INTERVAL_SECONDS, DEFAULT_POLL_INTERVAL_SECONDS);
            scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "sap-cdc-poller-" + sourceName);
                t.setDaemon(true);
                return t;
            });
            
            scheduler.scheduleAtFixedRate(
                this::pollForChanges,
                60, // Initial delay
                pollIntervalSeconds,
                TimeUnit.SECONDS
            );
        }
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            LOG.info("Stopped SAP CDC Source Connector: " + sourceName);
        }
    }

    @Override
    public void shutdown() {
        stop();
        
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        if (joinProcessor != null) {
            joinProcessor.shutdown();
        }
        
        if (transactionManager != null) {
            transactionManager.shutdown();
        }
        
        if (protocolHandler != null) {
            protocolHandler.shutdown();
        }
        
        LOG.info("SAP CDC Source Connector shut down: " + sourceName);
    }

    /**
     * Polling method - runs periodically to poll for changes.
     */
    public void pollForChanges() {
        if (!running.get()) {
            return;
        }

        if (!protocolHandler.isHealthy()) {
            LOG.warn("Protocol handler is not healthy, skipping poll cycle");
            if (pollErrors != null) pollErrors.increment();
            return;
        }

        Runnable pollTask = () -> {
            try {
                int batchSize = getConfigInt(BATCH_SIZE, DEFAULT_BATCH_SIZE);
                long currentOffset = lastOffset.get();
                
                LOG.debug("Polling for changes, offset: " + currentOffset + ", batch size: " + batchSize);
                
                List<ChangeRecord> changes = protocolHandler.pollChanges(currentOffset, batchSize);
                
                if (changes.isEmpty()) {
                    LOG.debug("No changes found in poll cycle");
                    return;
                }

                LOG.info("Found " + changes.size() + " change records");
                
                // Process changes
                processChanges(changes);
                
                // Update offset to the last processed record
                if (!changes.isEmpty()) {
                    ChangeRecord lastChange = changes.get(changes.size() - 1);
                    if (lastChange.getOffset() != null) {
                        lastOffset.set(lastChange.getOffset());
                    }
                }
                
            } catch (Exception e) {
                LOG.error("Error during poll cycle", e);
                if (pollErrors != null) pollErrors.increment();
            }
        };

        if (pollDuration != null) {
            pollDuration.record(pollTask);
        } else {
            pollTask.run();
        }
    }

    /**
     * Process a batch of change records.
     */
    protected void processChanges(List<ChangeRecord> changes) throws IOException {
        for (ChangeRecord change : changes) {
            String transactionId = change.getTransactionId();
            
            if (transactionId == null || transactionId.isEmpty()) {
                // No transaction grouping, process immediately
                processCommittedChange(change);
            } else {
                // Group by transaction
                transactionManager.addChange(change);
                
                // Check if transaction is committed (this would need to be determined by protocol handler)
                // For now, we'll process after a delay or when we detect commit
                // In a real implementation, the protocol handler would indicate transaction status
            }
        }
        
        // Process any committed transactions
        processCommittedTransactions();
    }

    /**
     * Process a single committed change record.
     */
    protected void processCommittedChange(ChangeRecord change) throws IOException {
        // Enrich with JOINs if configured
        ChangeRecord enriched = joinProcessor.enrichWithJoins(change);
        
        // Determine event name (typically from table name)
        String eventName = getEventName(enriched.getTableName());
        
        // Get SRN (Schema Resource Name) - could be from config or inferred
        String srn = getSrn(enriched.getTableName());
        
        // Get host identifier
        String host = context.sourceConfig().getOrDefault(HOST, DEFAULT_HOST);
        
        // Inject event into EventQueue
        eventInjector.injectEvent(enriched, eventName, srn, host);
        
        if (eventsProcessed != null) {
            eventsProcessed.increment();
        }
    }

    /**
     * Process committed transactions.
     * Optimized to only check transactions that might be ready for commit.
     */
    protected void processCommittedTransactions() throws IOException {
        // Get all in-progress transactions
        List<TransactionState> transactions = transactionManager.getInProgressTransactions();
        
        if (transactions.isEmpty()) {
            return;
        }
        
        long durationLimitMs = getConfigLong(TRANSACTION_DURATION_LIMIT_SECONDS, DEFAULT_DURATION_LIMIT_SECONDS) * 1000;
        long currentTime = System.currentTimeMillis();
        
        // Filter transactions that are ready for commit (older than duration limit)
        List<String> transactionsToCommit = new ArrayList<>();
        for (TransactionState transaction : transactions) {
            if (currentTime - transaction.getStartTime() > durationLimitMs) {
                transactionsToCommit.add(transaction.getTransactionId());
            }
        }
        
        // Process commits in batch
        for (String transactionId : transactionsToCommit) {
            try {
                List<ChangeRecord> committedChanges = transactionManager.commitTransaction(transactionId);
                
                // Process changes in batch for better performance
                for (ChangeRecord change : committedChanges) {
                    processCommittedChange(change);
                }
            } catch (Exception e) {
                LOG.error("Failed to commit transaction: " + transactionId, e);
                // Continue with other transactions
            }
        }
    }

    /**
     * Get event name from table name.
     * Can be overridden by subclasses for custom mapping.
     */
    protected String getEventName(String tableName) {
        // Default: use table name as event name
        // Could be configured via mapping
        return tableName.toLowerCase().replace("_", "");
    }

    /**
     * Get SRN (Schema Resource Name) for a table.
     * Can be overridden by subclasses for custom mapping.
     */
    protected String getSrn(String tableName) {
        // Default: construct SRN from table name
        // Format: srn:organization:domain:schemaName:schemaVersion
        return "srn:nobu:sap:" + tableName.toLowerCase() + ":1-0-0";
    }

    /**
     * Create protocol-specific handler.
     * Must be implemented by subclasses.
     */
    protected abstract ProtocolHandler createProtocolHandler();

    /**
     * Get the source type of this connector.
     * Must be implemented by subclasses.
     */
    @Override
    public abstract SourceType getSourceType();

    /**
     * Parse JOIN configurations from source config.
     */
    protected Map<String, JoinProcessor.JoinConfig> parseJoinConfigs() {
        // TODO: Parse join.tables configuration from YAML
        // For now, return empty map
        return Map.of();
    }

    /**
     * Get configuration value as integer.
     */
    protected int getConfigInt(String key, int defaultValue) {
        String value = context.sourceConfig().get(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            LOG.warn("Invalid integer value for config key: " + key + ", using default: " + defaultValue);
            return defaultValue;
        }
    }

    /**
     * Get configuration value as long.
     */
    protected long getConfigLong(String key, long defaultValue) {
        String value = context.sourceConfig().get(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            LOG.warn("Invalid long value for config key: " + key + ", using default: " + defaultValue);
            return defaultValue;
        }
    }
}
