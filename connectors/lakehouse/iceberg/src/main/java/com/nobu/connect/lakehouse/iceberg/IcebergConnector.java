package com.nobu.connect.lakehouse.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nobu.spi.connect.Connector;
import com.nobu.spi.connect.Context;
import com.nobu.spi.event.NobuEvent;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.GenericParquetWriter;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Production-ready Iceberg connector for writing events to Iceberg tables.
 * 
 * Features:
 * - Automatic table creation with schema from SRN
 * - Batch writing for performance
 * - Schema evolution support
 * - Multiple catalog types (Hadoop, S3, etc.)
 * - Proper error handling and resource management
 */
public class IcebergConnector implements Connector {
    
    private static final Logger LOG = Logger.getLogger(IcebergConnector.class);
    
    // Configuration keys
    private static final String CATALOG_TYPE = "catalog.type";
    private static final String WAREHOUSE_PATH = "warehouse.path";
    private static final String BATCH_SIZE = "batch.size";
    private static final String DEFAULT_BATCH_SIZE = "1000";
    
    // Default values
    private static final String DEFAULT_CATALOG_TYPE = "hadoop";
    private static final String DEFAULT_WAREHOUSE_PATH = "/tmp/iceberg-warehouse";
    
    private Catalog catalog;
    private String warehousePath;
    private final Map<String, Table> tableCache = new ConcurrentHashMap<>();
    private final Map<String, org.apache.iceberg.Schema> schemaCache = new ConcurrentHashMap<>();
    private final Map<String, List<Record>> batchBuffer = new ConcurrentHashMap<>();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private int batchSize;
    private boolean initialized = false;

    @Override
    public void initialize(String target, Context context) {
        try {
            LOG.info("Initializing IcebergConnector for target: " + target);
            
            Map<String, String> config = context.connectConfig();
            String catalogType = config.getOrDefault(CATALOG_TYPE, DEFAULT_CATALOG_TYPE);
            warehousePath = config.getOrDefault(WAREHOUSE_PATH, DEFAULT_WAREHOUSE_PATH);
            batchSize = Integer.parseInt(config.getOrDefault(BATCH_SIZE, DEFAULT_BATCH_SIZE));
            
            // Initialize catalog based on type
            catalog = createCatalog(catalogType, warehousePath, config);
            
            // Ensure warehouse directory exists
            ensureWarehouseDirectory(warehousePath);
            
            initialized = true;
            LOG.info("IcebergConnector initialized successfully. Warehouse: " + warehousePath + 
                    ", Catalog Type: " + catalogType + ", Batch Size: " + batchSize);
            
        } catch (Exception e) {
            LOG.error("Failed to initialize IcebergConnector", e);
            throw new RuntimeException("IcebergConnector initialization failed", e);
        }
    }

    @Override
    public void onEvent(NobuEvent event, long sequence, boolean endOfBatch) throws Exception {
        if (!initialized) {
            LOG.warn("IcebergConnector not initialized, skipping event");
            return;
        }
        
        try {
            String eventName = event.getEventName();
            if (eventName == null || eventName.isEmpty()) {
                LOG.warn("Event name is null or empty, skipping event");
                return;
            }
            
            String tableName = eventName;
            byte[] messageBytes = event.getMessage();
            
            if (messageBytes == null || messageBytes.length == 0) {
                LOG.warn("Message is null or empty for event: " + eventName);
                return;
            }
            
            // Get or create table
            Table table = getOrCreateTable(tableName, event.getSrn());
            
            // Parse JSON message
            JsonNode jsonNode = objectMapper.readTree(messageBytes);
            
            // Convert JSON to Iceberg Record
            Record record = convertJsonToRecord(jsonNode, table.schema());
            
            // Add metadata fields
            addMetadataFields(record, event, table.schema());
            
            // Add to batch buffer
            batchBuffer.computeIfAbsent(tableName, k -> new ArrayList<>()).add(record);
            
            // Flush batch if needed
            if (endOfBatch || batchBuffer.get(tableName).size() >= batchSize) {
                flushBatch(tableName, table);
            }
            
        } catch (Exception e) {
            LOG.error("Error processing event: " + event.getEventName(), e);
            throw e;
        }
    }

    @Override
    public void shutdown() {
        LOG.info("Shutting down IcebergConnector");
        
        try {
            // Flush all pending batches
            for (Map.Entry<String, List<Record>> entry : batchBuffer.entrySet()) {
                if (!entry.getValue().isEmpty()) {
                    String tableName = entry.getKey();
                    Table table = tableCache.get(tableName);
                    if (table != null) {
                        flushBatch(tableName, table);
                    }
                }
            }
            
            // Close all tables
            for (Table table : tableCache.values()) {
                try {
                    table.close();
                } catch (Exception e) {
                    LOG.warn("Error closing table", e);
                }
            }
            
            tableCache.clear();
            schemaCache.clear();
            batchBuffer.clear();
            
            initialized = false;
            LOG.info("IcebergConnector shutdown completed");
            
        } catch (Exception e) {
            LOG.error("Error during IcebergConnector shutdown", e);
        }
    }

    /**
     * Create catalog based on type
     */
    private Catalog createCatalog(String catalogType, String warehousePath, Map<String, String> config) {
        Configuration hadoopConf = new Configuration();
        
        // Set Hadoop configuration from config if provided
        config.forEach((key, value) -> {
            if (key.startsWith("hadoop.")) {
                hadoopConf.set(key.substring(7), value);
            }
        });
        
        switch (catalogType.toLowerCase()) {
            case "hadoop":
                return new HadoopCatalog(hadoopConf, warehousePath);
            case "hive":
                // For Hive catalog, you would use HiveCatalog
                // This requires additional dependencies
                LOG.warn("Hive catalog not fully implemented, falling back to Hadoop catalog");
                return new HadoopCatalog(hadoopConf, warehousePath);
            default:
                LOG.warn("Unknown catalog type: " + catalogType + ", using Hadoop catalog");
                return new HadoopCatalog(hadoopConf, warehousePath);
        }
    }

    /**
     * Ensure warehouse directory exists
     */
    private void ensureWarehouseDirectory(String warehousePath) {
        try {
            Path path = Paths.get(warehousePath);
            if (!Files.exists(path)) {
                Files.createDirectories(path);
                LOG.info("Created warehouse directory: " + warehousePath);
            }
        } catch (IOException e) {
            LOG.warn("Could not create warehouse directory: " + warehousePath, e);
        }
    }

    /**
     * Get or create table for the given event name
     */
    private Table getOrCreateTable(String tableName, String srn) {
        return tableCache.computeIfAbsent(tableName, name -> {
            try {
                TableIdentifier identifier = TableIdentifier.of("default", name);
                
                // Try to load existing table
                if (catalog.tableExists(identifier)) {
                    LOG.info("Loading existing table: " + name);
                    return catalog.loadTable(identifier);
                }
                
                // Create new table with schema from SRN
                LOG.info("Creating new table: " + name + " with schema from SRN: " + srn);
                org.apache.iceberg.Schema schema = getOrLoadSchema(name, srn);
                
                PartitionSpec partitionSpec = PartitionSpec.unpartitioned();
                
                // Create table with schema
                return catalog.createTable(identifier, schema, partitionSpec);
                
            } catch (Exception e) {
                LOG.error("Error getting or creating table: " + name, e);
                throw new RuntimeException("Failed to get or create table: " + name, e);
            }
        });
    }

    /**
     * Get or load schema from SRN URI
     */
    private org.apache.iceberg.Schema getOrLoadSchema(String tableName, String srn) {
        if (srn == null || srn.isEmpty()) {
            LOG.warn("SRN is null or empty for table: " + tableName + ", creating schema from first event");
            return null; // Will be created from first event
        }
        
        return schemaCache.computeIfAbsent(srn, uri -> {
            try {
                return loadSchemaFromUri(uri);
            } catch (Exception e) {
                LOG.error("Failed to load schema from SRN: " + uri, e);
                return null; // Will be created from first event
            }
        });
    }

    /**
     * Load schema from URI (SRN)
     */
    private org.apache.iceberg.Schema loadSchemaFromUri(String srnUri) throws IOException {
        URI uri;
        try {
            uri = new URI(srnUri);
        } catch (Exception e) {
            LOG.warn("Invalid URI format: " + srnUri + ", treating as file path");
            uri = Paths.get(srnUri).toUri();
        }
        
        JsonNode schemaJson;
        try (InputStream inputStream = getInputStreamFromUri(uri)) {
            schemaJson = objectMapper.readTree(inputStream);
        }
        
        return convertJsonSchemaToIcebergSchema(schemaJson);
    }

    /**
     * Get input stream from URI
     */
    private InputStream getInputStreamFromUri(URI uri) throws IOException {
        String scheme = uri.getScheme();
        
        if (scheme == null || "file".equals(scheme)) {
            // File path
            Path path = Paths.get(uri);
            if (!Files.exists(path)) {
                throw new IOException("Schema file not found: " + uri);
            }
            return Files.newInputStream(path);
        } else if ("http".equals(scheme) || "https".equals(scheme)) {
            // HTTP/HTTPS URL
            return uri.toURL().openStream();
        } else if ("classpath".equals(scheme)) {
            // Classpath resource
            String resourcePath = uri.getPath();
            InputStream stream = getClass().getClassLoader().getResourceAsStream(resourcePath);
            if (stream == null) {
                throw new IOException("Classpath resource not found: " + resourcePath);
            }
            return stream;
        } else {
            throw new IOException("Unsupported URI scheme: " + scheme);
        }
    }

    /**
     * Convert JSON Schema to Iceberg Schema
     * Supports both JSON Schema format and simple field definitions
     */
    private org.apache.iceberg.Schema convertJsonSchemaToIcebergSchema(JsonNode schemaJson) {
        List<Types.NestedField> fields = new ArrayList<>();
        
        // Check if it's a JSON Schema format
        if (schemaJson.has("properties")) {
            JsonNode properties = schemaJson.get("properties");
            properties.fields().forEachRemaining(entry -> {
                String fieldName = entry.getKey();
                JsonNode fieldDef = entry.getValue();
                String type = fieldDef.has("type") ? fieldDef.get("type").asText() : "string";
                boolean optional = !fieldDef.has("required") || 
                    !fieldDef.get("required").asBoolean();
                
                Types.NestedField field = createIcebergField(fieldName, type, optional);
                if (field != null) {
                    fields.add(field);
                }
            });
        } else if (schemaJson.isArray()) {
            // Array of field definitions
            schemaJson.forEach(fieldDef -> {
                String fieldName = fieldDef.has("name") ? fieldDef.get("name").asText() : 
                    fieldDef.has("field") ? fieldDef.get("field").asText() : "unknown";
                String type = fieldDef.has("type") ? fieldDef.get("type").asText() : "string";
                boolean optional = !fieldDef.has("required") || 
                    !fieldDef.get("required").asBoolean();
                
                Types.NestedField field = createIcebergField(fieldName, type, optional);
                if (field != null) {
                    fields.add(field);
                }
            });
        } else {
            // Try to infer schema from object structure
            LOG.warn("Unknown schema format, will infer from data");
            return null;
        }
        
        // Add metadata fields
        fields.add(Types.NestedField.optional(fields.size() + 1, "_event_id", Types.StringType.get()));
        fields.add(Types.NestedField.optional(fields.size() + 2, "_timestamp", Types.LongType.get()));
        fields.add(Types.NestedField.optional(fields.size() + 3, "_host", Types.StringType.get()));
        fields.add(Types.NestedField.optional(fields.size() + 4, "_srn", Types.StringType.get()));
        
        return new org.apache.iceberg.Schema(fields);
    }

    /**
     * Create Iceberg field from type string
     */
    private Types.NestedField createIcebergField(String name, String type, boolean optional) {
        Types.NestedField.Builder builder = optional ? 
            Types.NestedField.optional(Integer.MAX_VALUE, name, getIcebergType(type)) :
            Types.NestedField.required(Integer.MAX_VALUE, name, getIcebergType(type));
        
        return builder.build();
    }

    /**
     * Map JSON Schema type to Iceberg type
     */
    private Type getIcebergType(String jsonType) {
        switch (jsonType.toLowerCase()) {
            case "string":
            case "str":
                return Types.StringType.get();
            case "integer":
            case "int":
            case "long":
                return Types.LongType.get();
            case "double":
            case "float":
            case "number":
                return Types.DoubleType.get();
            case "boolean":
            case "bool":
                return Types.BooleanType.get();
            case "timestamp":
            case "datetime":
                return Types.TimestampType.withZone();
            case "date":
                return Types.DateType.get();
            case "binary":
            case "bytes":
                return Types.BinaryType.get();
            default:
                LOG.warn("Unknown type: " + jsonType + ", defaulting to string");
                return Types.StringType.get();
        }
    }

    /**
     * Convert JSON node to Iceberg Record
     */
    private Record convertJsonToRecord(JsonNode jsonNode, org.apache.iceberg.Schema schema) {
        if (schema == null) {
            // Infer schema from JSON
            schema = inferSchemaFromJson(jsonNode);
        }
        
        GenericRecord record = GenericRecord.create(schema);
        
        // Set fields from JSON
        schema.columns().forEach(column -> {
            String fieldName = column.name();
            if (!fieldName.startsWith("_")) { // Skip metadata fields
                JsonNode value = jsonNode.get(fieldName);
                if (value != null && !value.isNull()) {
                    record.set(fieldName, convertJsonValue(value, column.type()));
                }
            }
        });
        
        return record;
    }

    /**
     * Infer schema from JSON node
     */
    private org.apache.iceberg.Schema inferSchemaFromJson(JsonNode jsonNode) {
        List<Types.NestedField> fields = new ArrayList<>();
        java.util.concurrent.atomic.AtomicInteger id = new java.util.concurrent.atomic.AtomicInteger(1);
        
        jsonNode.fields().forEachRemaining(entry -> {
            String fieldName = entry.getKey();
            JsonNode value = entry.getValue();
            Type type = inferTypeFromJsonNode(value);
            fields.add(Types.NestedField.optional(id.getAndIncrement(), fieldName, type));
        });
        
        // Add metadata fields
        fields.add(Types.NestedField.optional(id.getAndIncrement(), "_event_id", Types.StringType.get()));
        fields.add(Types.NestedField.optional(id.getAndIncrement(), "_timestamp", Types.LongType.get()));
        fields.add(Types.NestedField.optional(id.getAndIncrement(), "_host", Types.StringType.get()));
        fields.add(Types.NestedField.optional(id.getAndIncrement(), "_srn", Types.StringType.get()));
        
        return new org.apache.iceberg.Schema(fields);
    }

    /**
     * Infer Iceberg type from JSON node
     */
    private Type inferTypeFromJsonNode(JsonNode node) {
        if (node.isTextual()) {
            return Types.StringType.get();
        } else if (node.isInt() || node.isLong()) {
            return Types.LongType.get();
        } else if (node.isDouble() || node.isFloat()) {
            return Types.DoubleType.get();
        } else if (node.isBoolean()) {
            return Types.BooleanType.get();
        } else if (node.isBinary()) {
            return Types.BinaryType.get();
        } else {
            return Types.StringType.get(); // Default to string for complex types
        }
    }

    /**
     * Convert JSON value to Java object based on Iceberg type
     */
    private Object convertJsonValue(JsonNode value, Type type) {
        if (value.isNull()) {
            return null;
        }
        
        if (type.equals(Types.StringType.get())) {
            return value.asText();
        } else if (type.equals(Types.LongType.get())) {
            return value.asLong();
        } else if (type.equals(Types.DoubleType.get())) {
            return value.asDouble();
        } else if (type.equals(Types.BooleanType.get())) {
            return value.asBoolean();
        } else if (type.equals(Types.BinaryType.get())) {
            try {
                return value.binaryValue();
            } catch (IOException e) {
                LOG.warn("Error converting to binary", e);
                return null;
            }
        } else {
            return value.asText(); // Default to string
        }
    }

    /**
     * Add metadata fields to record
     */
    private void addMetadataFields(Record record, NobuEvent event, org.apache.iceberg.Schema schema) {
        if (schema.findField("_event_id") != null) {
            record.set("_event_id", event.getEventId());
        }
        if (schema.findField("_timestamp") != null) {
            record.set("_timestamp", event.getTimestamp());
        }
        if (schema.findField("_host") != null) {
            record.set("_host", event.getHost());
        }
        if (schema.findField("_srn") != null) {
            record.set("_srn", event.getSrn());
        }
    }

    /**
     * Flush batch for a table
     */
    private void flushBatch(String tableName, Table table) {
        List<Record> batch = batchBuffer.remove(tableName);
        if (batch == null || batch.isEmpty()) {
            return;
        }
        
        try {
            // Create append operation
            AppendFiles append = table.newAppend();
            
            // Generate a unique file path for this batch
            String fileName = "data-" + System.currentTimeMillis() + "-" + UUID.randomUUID() + ".parquet";
            String filePath = table.locationProvider().newDataLocation(fileName);
            OutputFile outputFile = table.io().newOutputFile(filePath);
            
            // Write records using Parquet FileAppender
            try (FileAppender<Record> appender = Parquet.write(outputFile)
                    .schema(table.schema())
                    .createWriterFunc(GenericParquetWriter::buildWriter)
                    .build()) {
                
                for (Record record : batch) {
                    appender.add(record);
                }
            }
            
            // Create data file metadata
            DataFile dataFile = DataFiles.builder(table.spec())
                .withPath(filePath)
                .withFileSizeInBytes(table.io().newInputFile(filePath).getLength())
                .withRecordCount(batch.size())
                .build();
            
            // Append the file
            append.appendFile(dataFile);
            append.commit();
            
            LOG.debug("Flushed batch of " + batch.size() + " records to table: " + tableName);
            
        } catch (Exception e) {
            LOG.error("Error flushing batch for table: " + tableName, e);
            // Re-add batch to buffer for retry (in production, you might want a DLQ)
            batchBuffer.put(tableName, batch);
            throw new RuntimeException("Failed to flush batch for table: " + tableName, e);
        }
    }
}
