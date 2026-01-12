package com.nobu.connect.sap;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.nobu.connect.sap.model.ChangeRecord;
import com.nobu.connect.sap.protocol.ProtocolHandler;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.jboss.logging.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Handles JOIN operations with source SAP tables for data enrichment.
 * Supports caching and different JOIN strategies per protocol type.
 * Uses Caffeine for high-performance caching and Micrometer for observability.
 */
public class JoinProcessor {
    private static final Logger LOG = Logger.getLogger(JoinProcessor.class);
    
    private final ProtocolHandler protocolHandler;
    private final Map<String, JoinConfig> joinConfigs;
    private final Cache<String, Map<String, Object>> joinCache;
    private final boolean joinEnabled;
    private final MeterRegistry meterRegistry;

    // Metrics
    private final Counter joinHits;
    private final Counter joinMisses;
    private final Counter joinErrors;
    private final Timer joinLatency;

    public JoinProcessor(ProtocolHandler protocolHandler, Map<String, JoinConfig> joinConfigs, boolean joinEnabled, MeterRegistry meterRegistry) {
        this(protocolHandler, joinConfigs, joinEnabled, 10000, meterRegistry);
    }

    public JoinProcessor(ProtocolHandler protocolHandler, Map<String, JoinConfig> joinConfigs, 
                        boolean joinEnabled, long maxCacheSize, MeterRegistry meterRegistry) {
        this.protocolHandler = protocolHandler;
        this.joinConfigs = joinConfigs != null ? joinConfigs : new HashMap<>();
        this.joinEnabled = joinEnabled;
        this.meterRegistry = meterRegistry; // Can be null if metrics are disabled
        
        // Initialize Caffeine Cache
        if (joinEnabled) {
            this.joinCache = Caffeine.newBuilder()
                .maximumSize(maxCacheSize)
                // Use custom Expiry to support per-table TTL
                .expireAfter(new com.github.benmanes.caffeine.cache.Expiry<String, Map<String, Object>>() {
                    @Override
                    public long expireAfterCreate(String key, Map<String, Object> value, long currentTime) {
                        // Find table from key (format table:key:value)
                        String table = key.split(":")[0];
                        JoinConfig config = JoinProcessor.this.joinConfigs.get(table);
                        if (config != null && config.getCacheTtlSeconds() > 0) {
                            return TimeUnit.SECONDS.toNanos(config.getCacheTtlSeconds());
                        }
                        return TimeUnit.MINUTES.toNanos(5); // Default
                    }

                    @Override
                    public long expireAfterUpdate(String key, Map<String, Object> value, long currentTime, long currentDuration) {
                        return currentDuration;
                    }

                    @Override
                    public long expireAfterRead(String key, Map<String, Object> value, long currentTime, long currentDuration) {
                        return currentDuration;
                    }
                })
                .recordStats()
                .build();
        } else {
            this.joinCache = null;
        }

        // Initialize Metrics (only if registry is available)
        if (meterRegistry != null) {
            this.joinHits = Counter.builder("sap.cdc.join.hits")
                .description("Number of join cache hits")
                .register(meterRegistry);
            this.joinMisses = Counter.builder("sap.cdc.join.misses")
                .description("Number of join cache misses")
                .register(meterRegistry);
            this.joinErrors = Counter.builder("sap.cdc.join.errors")
                .description("Number of join errors")
                .register(meterRegistry);
            this.joinLatency = Timer.builder("sap.cdc.join.latency")
                .description("Time taken to perform join operations")
                .register(meterRegistry);
        } else {
            // Create no-op metrics if registry is not available
            io.micrometer.core.instrument.simple.SimpleMeterRegistry noOpRegistry = 
                new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
            this.joinHits = Counter.builder("sap.cdc.join.hits").register(noOpRegistry);
            this.joinMisses = Counter.builder("sap.cdc.join.misses").register(noOpRegistry);
            this.joinErrors = Counter.builder("sap.cdc.join.errors").register(noOpRegistry);
            this.joinLatency = Timer.builder("sap.cdc.join.latency").register(noOpRegistry);
        }
    }

    /**
     * Enrich a change record with JOINed data.
     */
    public ChangeRecord enrichWithJoins(ChangeRecord changeRecord) {
        if (!joinEnabled || joinConfigs.isEmpty()) {
            return changeRecord;
        }

        JoinConfig config = joinConfigs.get(changeRecord.getTableName());
        if (config == null) {
            return changeRecord;
        }

        return joinLatency.record(() -> {
            try {
                // Get join value from change record
                Object joinValue = getJoinValue(changeRecord, config.getJoinKey());
                if (joinValue == null) {
                    return changeRecord;
                }

                // Check cache first
                String cacheKey = buildCacheKey(config.getTable(), config.getJoinKey(), joinValue);
                
                Map<String, Object> joinedData = joinCache.getIfPresent(cacheKey);
                
                if (joinedData != null) {
                    joinHits.increment();
                    return mergeJoinData(changeRecord, joinedData);
                }
                
                joinMisses.increment();
                
                // Cache miss - perform JOIN
                joinedData = protocolHandler.performJoin(config.getTable(), config.getJoinKey(), joinValue);
                
                if (joinedData != null && !joinedData.isEmpty()) {
                    joinCache.put(cacheKey, joinedData);
                    return mergeJoinData(changeRecord, joinedData);
                }

            } catch (Exception e) {
                joinErrors.increment();
                LOG.warn("Failed to perform JOIN for table: " + changeRecord.getTableName(), e);
            }
            return changeRecord;
        });
    }

    // ... helper methods ...

    /**
     * Get join value from change record.
     */
    private Object getJoinValue(ChangeRecord changeRecord, String joinKey) {
        Map<String, Object> values = changeRecord.getAfterValues();
        if (values == null || !values.containsKey(joinKey)) {
            values = changeRecord.getBeforeValues();
        }
        return values != null ? values.get(joinKey) : null;
    }

    /**
     * Merge joined data into change record.
     */
    private ChangeRecord mergeJoinData(ChangeRecord original, Map<String, Object> joinedData) {
        ChangeRecord enriched = new ChangeRecord();
        enriched.setTransactionId(original.getTransactionId());
        enriched.setTableName(original.getTableName());
        enriched.setChangeType(original.getChangeType());
        enriched.setTimestamp(original.getTimestamp());
        enriched.setOperationId(original.getOperationId());
        enriched.setOffset(original.getOffset());

        if (original.getAfterValues() != null) {
            Map<String, Object> mergedAfter = new HashMap<>(original.getAfterValues());
            mergedAfter.putAll(joinedData);
            enriched.setAfterValues(mergedAfter);
        }

        if (original.getBeforeValues() != null) {
            Map<String, Object> mergedBefore = new HashMap<>(original.getBeforeValues());
            mergedBefore.putAll(joinedData);
            enriched.setBeforeValues(mergedBefore);
        }

        return enriched;
    }

    /**
     * Build cache key for join result.
     */
    private String buildCacheKey(String table, String joinKey, Object joinValue) {
        return table + ":" + joinKey + ":" + (joinValue != null ? joinValue.toString() : "null");
    }

    public void clearCache() {
        if (joinCache != null) {
            joinCache.invalidateAll();
        }
    }
    
    public void shutdown() {
        clearCache();
    }

    public static class JoinConfig {
        private final String table;
        private final String joinKey;
        private final long cacheTtlSeconds;

        public JoinConfig(String table, String joinKey, long cacheTtlSeconds) {
            this.table = table;
            this.joinKey = joinKey;
            this.cacheTtlSeconds = cacheTtlSeconds;
        }

        public String getTable() { return table; }
        public String getJoinKey() { return joinKey; }
        public long getCacheTtlSeconds() { return cacheTtlSeconds; }
    }
}
