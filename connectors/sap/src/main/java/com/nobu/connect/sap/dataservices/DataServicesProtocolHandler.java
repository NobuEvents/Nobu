package com.nobu.connect.sap.dataservices;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nobu.connect.sap.model.ChangeRecord;
import com.nobu.connect.sap.model.ChangeType;
import com.nobu.connect.sap.protocol.ProtocolHandler;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.util.*;

/**
 * SAP Data Services protocol handler.
 * Integrates with SAP Data Services replication server for CDC.
 */
public class DataServicesProtocolHandler implements ProtocolHandler {
    private static final Logger LOG = Logger.getLogger(DataServicesProtocolHandler.class);
    
    private final ObjectMapper objectMapper;
    private StreamSubscriptionManager subscriptionManager;
    
    private String serverUrl;
    private String subscriptionName;
    private String replicationStream;
    private String mode;
    private int batchSize;
    private boolean initialized = false;

    // Configuration keys
    private static final String DS_SERVER_URL = "ds.server.url";
    private static final String DS_SUBSCRIPTION_NAME = "ds.subscription.name";
    private static final String DS_REPLICATION_STREAM = "ds.replication.stream";
    private static final String MODE = "mode";
    private static final String BATCH_SIZE = "batch.size";

    public DataServicesProtocolHandler() {
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void initialize(Map<String, String> config) {
        if (initialized) {
            return;
        }

        this.serverUrl = config.get(DS_SERVER_URL);
        this.subscriptionName = config.get(DS_SUBSCRIPTION_NAME);
        this.replicationStream = config.get(DS_REPLICATION_STREAM);
        this.mode = config.getOrDefault(MODE, "pull");
        this.batchSize = Integer.parseInt(config.getOrDefault(BATCH_SIZE, "1000"));

        if (serverUrl == null || subscriptionName == null || replicationStream == null) {
            throw new IllegalArgumentException("Data Services server URL, subscription name, and replication stream are required");
        }

        this.subscriptionManager = new StreamSubscriptionManager(
            serverUrl, subscriptionName, replicationStream, mode
        );

        try {
            subscriptionManager.subscribe();
        } catch (IOException e) {
            LOG.error("Failed to subscribe to Data Services stream", e);
            throw new RuntimeException("Failed to initialize Data Services subscription", e);
        }

        initialized = true;
        LOG.info("Data Services Protocol Handler initialized for subscription: " + subscriptionName);
    }

    @Override
    public List<ChangeRecord> pollChanges(long lastOffset, int batchSize) {
        if (!initialized) {
            throw new IllegalStateException("Protocol handler not initialized");
        }

        List<ChangeRecord> changes = new ArrayList<>();

        try {
            // Pull events from Data Services
            String eventsJson = subscriptionManager.pullEvents(batchSize);
            
            if (eventsJson == null || eventsJson.isEmpty()) {
                return changes;
            }

            // Parse Data Services event format
            JsonNode jsonResponse = objectMapper.readTree(eventsJson);
            
            // Data Services format may vary - handle common structures
            JsonNode eventsArray = jsonResponse;
            if (jsonResponse.has("events")) {
                eventsArray = jsonResponse.get("events");
            } else if (jsonResponse.has("value")) {
                eventsArray = jsonResponse.get("value");
            } else if (jsonResponse.isArray()) {
                eventsArray = jsonResponse;
            }

            if (eventsArray != null && eventsArray.isArray()) {
                for (JsonNode event : eventsArray) {
                    ChangeRecord record = mapDataServicesEventToChangeRecord(event);
                    if (record != null) {
                        changes.add(record);
                    }
                }
            }

        } catch (IOException e) {
            LOG.error("Failed to poll changes from Data Services", e);
            throw new RuntimeException("Failed to poll CDC changes", e);
        }

        return changes;
    }

    @Override
    public Map<String, Object> performJoin(String tableName, String joinKey, Object joinValue) {
        // Data Services typically handles JOINs in the replication stream itself
        // If needed, we could make a separate API call, but for now return empty
        LOG.debug("JOIN operations are typically handled by Data Services replication stream");
        return Collections.emptyMap();
    }

    @Override
    public void shutdown() {
        if (subscriptionManager != null) {
            subscriptionManager.unsubscribe();
        }
        initialized = false;
        LOG.info("Data Services Protocol Handler shut down");
    }

    @Override
    public boolean isHealthy() {
        if (!initialized) {
            return false;
        }

        // Check if subscription is active
        return subscriptionManager != null && subscriptionManager.isSubscribed();
    }

    /**
     * Map Data Services event format to ChangeRecord.
     */
    private ChangeRecord mapDataServicesEventToChangeRecord(JsonNode event) {
        ChangeRecord record = new ChangeRecord();

        // Data Services event format may vary - adapt based on actual format
        if (event.has("transactionId")) {
            record.setTransactionId(event.get("transactionId").asText());
        } else if (event.has("TransactionId")) {
            record.setTransactionId(event.get("TransactionId").asText());
        }

        if (event.has("operationId")) {
            long operationId = event.get("operationId").asLong();
            record.setOperationId(String.valueOf(operationId));
            record.setOffset(operationId);
        } else if (event.has("OperationId")) {
            long operationId = event.get("OperationId").asLong();
            record.setOperationId(String.valueOf(operationId));
            record.setOffset(operationId);
        }

        if (event.has("timestamp")) {
            record.setTimestamp(event.get("timestamp").asLong());
        } else if (event.has("Timestamp")) {
            record.setTimestamp(event.get("Timestamp").asLong());
        }

        if (event.has("tableName")) {
            record.setTableName(event.get("tableName").asText());
        } else if (event.has("TableName")) {
            record.setTableName(event.get("TableName").asText());
        }

        if (event.has("operationType")) {
            record.setChangeType(parseChangeType(event.get("operationType").asText()));
        } else if (event.has("OperationType")) {
            record.setChangeType(parseChangeType(event.get("OperationType").asText()));
        }

        // Extract data
        Map<String, Object> beforeValues = null;
        Map<String, Object> afterValues = null;

        if (event.has("before")) {
            beforeValues = mapJsonToMap(event.get("before"));
        } else if (event.has("Before")) {
            beforeValues = mapJsonToMap(event.get("Before"));
        }

        if (event.has("after")) {
            afterValues = mapJsonToMap(event.get("after"));
        } else if (event.has("After")) {
            afterValues = mapJsonToMap(event.get("After"));
        } else if (event.has("data")) {
            afterValues = mapJsonToMap(event.get("data"));
        } else if (event.has("Data")) {
            afterValues = mapJsonToMap(event.get("Data"));
        }

        record.setBeforeValues(beforeValues);
        record.setAfterValues(afterValues);

        return record;
    }

    /**
     * Map JSON node to Map.
     */
    private Map<String, Object> mapJsonToMap(JsonNode json) {
        Map<String, Object> map = new HashMap<>();

        if (json != null && json.isObject()) {
            json.fields().forEachRemaining(entry -> {
                String key = entry.getKey();
                JsonNode value = entry.getValue();

                if (value.isTextual()) {
                    map.put(key, value.asText());
                } else if (value.isNumber()) {
                    if (value.isInt()) {
                        map.put(key, value.asInt());
                    } else if (value.isLong()) {
                        map.put(key, value.asLong());
                    } else {
                        map.put(key, value.asDouble());
                    }
                } else if (value.isBoolean()) {
                    map.put(key, value.asBoolean());
                } else {
                    map.put(key, value.toString());
                }
            });
        }

        return map;
    }

    /**
     * Parse change type from string.
     */
    private ChangeType parseChangeType(String operationType) {
        if (operationType == null) {
            return ChangeType.UPDATE;
        }

        String upper = operationType.toUpperCase();
        if (upper.contains("INSERT") || upper.equals("I")) {
            return ChangeType.INSERT;
        } else if (upper.contains("UPDATE") || upper.equals("U")) {
            return ChangeType.UPDATE;
        } else if (upper.contains("DELETE") || upper.equals("D")) {
            return ChangeType.DELETE;
        }

        return ChangeType.UPDATE;
    }
}
