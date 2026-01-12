package com.nobu.connect.sap.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nobu.connect.sap.model.ChangeRecord;
import com.nobu.connect.sap.model.ChangeType;
import com.nobu.connect.sap.protocol.ProtocolHandler;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * REST protocol handler for SAP OData/REST APIs.
 * Polls CDC endpoints and performs JOINs via REST calls.
 */
public class RestProtocolHandler implements ProtocolHandler {
    private static final Logger LOG = Logger.getLogger(RestProtocolHandler.class);
    
    // Shared HttpClient instance for better connection pooling and performance
    private static final HttpClient SHARED_HTTP_CLIENT = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(30))
        .build();
    
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private OAuth2TokenManager tokenManager;
    
    private String baseUrl;
    private String serviceName;
    private String entitySet;
    private String authType;
    private int pageSize;
    private boolean initialized = false;

    // Configuration keys
    private static final String REST_BASE_URL = "rest.base.url";
    private static final String REST_SERVICE = "rest.service";
    private static final String REST_ENTITY_SET = "rest.entity.set";
    private static final String AUTH_TYPE = "auth.type";
    private static final String OAUTH2_TOKEN_URL = "oauth2.token.url";
    private static final String OAUTH2_CLIENT_ID = "oauth2.client.id";
    private static final String OAUTH2_CLIENT_SECRET = "oauth2.client.secret";
    private static final String PAGE_SIZE = "page.size";

    public RestProtocolHandler() {
        // Use shared HttpClient for connection pooling and better performance
        this.httpClient = SHARED_HTTP_CLIENT;
        this.objectMapper = new ObjectMapper();
    }

    // Constructor for testing
    protected RestProtocolHandler(HttpClient httpClient, OAuth2TokenManager tokenManager) {
        this.httpClient = httpClient;
        this.tokenManager = tokenManager;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void initialize(Map<String, String> config) {
        if (initialized) {
            return;
        }

        this.baseUrl = config.get(REST_BASE_URL);
        this.serviceName = config.getOrDefault(REST_SERVICE, "ChangeDataCaptureService");
        this.entitySet = config.getOrDefault(REST_ENTITY_SET, "ChangeLogs");
        this.authType = config.getOrDefault(AUTH_TYPE, "oauth2");
        this.pageSize = Integer.parseInt(config.getOrDefault(PAGE_SIZE, "1000"));

        if (baseUrl == null) {
            throw new IllegalArgumentException("REST base URL is required");
        }

        // Initialize authentication
        if ("oauth2".equalsIgnoreCase(authType)) {
            String tokenUrl = config.get(OAUTH2_TOKEN_URL);
            String clientId = config.get(OAUTH2_CLIENT_ID);
            String clientSecret = config.get(OAUTH2_CLIENT_SECRET);
            
            if (tokenUrl == null || clientId == null || clientSecret == null) {
                throw new IllegalArgumentException("OAuth2 token URL, client ID, and client secret are required");
            }
            
            // Only create if not already injected (for testing)
            if (this.tokenManager == null) {
                this.tokenManager = new OAuth2TokenManager(tokenUrl, clientId, clientSecret);
            }
        }

        initialized = true;
        LOG.info("REST Protocol Handler initialized for service: " + serviceName);
    }

    @Override
    public List<ChangeRecord> pollChanges(long lastOffset, int batchSize) {
        if (!initialized) {
            throw new IllegalStateException("Protocol handler not initialized");
        }

        List<ChangeRecord> changes = new ArrayList<>();
        String entityUrl = baseUrl + "/" + serviceName + "/" + entitySet;
        
        try {
            // Build OData query
            ODataQueryBuilder queryBuilder = new ODataQueryBuilder(entityUrl)
                .filterTimestampGreaterThan("Timestamp", Instant.ofEpochMilli(lastOffset))
                .orderBy("Timestamp")
                .top(Math.min(batchSize, pageSize));

            String queryUrl = queryBuilder.build();
            LOG.debug("Polling OData endpoint: " + queryUrl);

            // Make HTTP request
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(queryUrl))
                .timeout(Duration.ofSeconds(60));

            // Add authentication header
            if (tokenManager != null) {
                String token = tokenManager.getAccessToken();
                requestBuilder.header("Authorization", "Bearer " + token);
            }

            HttpRequest request = requestBuilder.GET().build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new IOException("Failed to poll changes. Status: " + response.statusCode() + 
                    ", Body: " + response.body());
            }

            // Parse OData response
            JsonNode jsonResponse = objectMapper.readTree(response.body());
            JsonNode valueArray = jsonResponse.get("value");
            
            if (valueArray != null && valueArray.isArray()) {
                for (JsonNode item : valueArray) {
                    ChangeRecord record = mapJsonToChangeRecord(item);
                    if (record != null) {
                        changes.add(record);
                    }
                }
            }

            // Handle pagination if needed
            JsonNode nextLink = jsonResponse.get("@odata.nextLink");
            if (nextLink != null && changes.size() < batchSize) {
                // Follow next link for more results
                // (Simplified - full implementation would handle pagination properly)
            }

        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            LOG.error("Failed to poll changes from REST API", e);
            throw new RuntimeException("Failed to poll CDC changes", e);
        }

        return changes;
    }

    @Override
    public Map<String, Object> performJoin(String tableName, String joinKey, Object joinValue) {
        if (!initialized) {
            throw new IllegalStateException("Protocol handler not initialized");
        }

        try {
            // Build OData query for JOIN
            String entityUrl = baseUrl + "/" + serviceName + "/" + tableName;
            ODataQueryBuilder queryBuilder = new ODataQueryBuilder(entityUrl)
                .filter(joinKey + " eq '" + joinValue + "'")
                .top(1);

            String queryUrl = queryBuilder.build();
            LOG.debug("Performing JOIN via OData: " + queryUrl);

            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(queryUrl))
                .timeout(Duration.ofSeconds(30));

            if (tokenManager != null) {
                String token = tokenManager.getAccessToken();
                requestBuilder.header("Authorization", "Bearer " + token);
            }

            HttpRequest request = requestBuilder.GET().build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                JsonNode jsonResponse = objectMapper.readTree(response.body());
                JsonNode valueArray = jsonResponse.get("value");
                
                if (valueArray != null && valueArray.isArray() && valueArray.size() > 0) {
                    return mapJsonToMap(valueArray.get(0));
                }
            }

        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            LOG.warn("Failed to perform JOIN via REST API for table: " + tableName, e);
        }

        return Collections.emptyMap();
    }

    @Override
    public void shutdown() {
        initialized = false;
        LOG.info("REST Protocol Handler shut down");
    }

    @Override
    public boolean isHealthy() {
        if (!initialized) {
            return false;
        }

        try {
            // Simple health check - try to get access token
            if (tokenManager != null) {
                tokenManager.getAccessToken();
            }
            return true;
        } catch (Exception e) {
            LOG.warn("Health check failed", e);
            return false;
        }
    }

    /**
     * Map JSON node to ChangeRecord.
     */
    private ChangeRecord mapJsonToChangeRecord(JsonNode json) {
        ChangeRecord record = new ChangeRecord();
        
        if (json.has("TransactionId")) {
            record.setTransactionId(json.get("TransactionId").asText());
        }
        
        if (json.has("OperationId")) {
            long operationId = json.get("OperationId").asLong();
            record.setOperationId(String.valueOf(operationId));
            record.setOffset(operationId);
        }
        
        if (json.has("Timestamp")) {
            String timestampStr = json.get("Timestamp").asText();
            // Parse OData datetime format
            Instant instant = Instant.parse(timestampStr);
            record.setTimestamp(instant.toEpochMilli());
        }
        
        if (json.has("TableName")) {
            record.setTableName(json.get("TableName").asText());
        }
        
        if (json.has("OperationType")) {
            record.setChangeType(parseChangeType(json.get("OperationType").asText()));
        }
        
        // Extract before/after values
        Map<String, Object> beforeValues = json.has("BeforeValues") 
            ? mapJsonToMap(json.get("BeforeValues"))
            : null;
        Map<String, Object> afterValues = json.has("AfterValues")
            ? mapJsonToMap(json.get("AfterValues"))
            : null;
        
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
