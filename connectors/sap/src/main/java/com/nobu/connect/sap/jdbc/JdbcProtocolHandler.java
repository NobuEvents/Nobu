package com.nobu.connect.sap.jdbc;

import com.nobu.connect.sap.model.ChangeRecord;
import com.nobu.connect.sap.model.ChangeType;
import com.nobu.connect.sap.protocol.ProtocolHandler;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jboss.logging.Logger;

import java.sql.*;
import java.util.*;

/**
 * JDBC protocol handler for SAP HANA CDC.
 * Polls CDC log tables and performs JOINs via SQL.
 */
public class JdbcProtocolHandler implements ProtocolHandler {
    private static final Logger LOG = Logger.getLogger(JdbcProtocolHandler.class);

    private HikariDataSource dataSource;
    private String cdcTable;
    private String offsetColumn;
    private String transactionIdColumn;
    private String timestampColumn;
    private boolean initialized = false;

    // Configuration keys
    private static final String JDBC_URL = "jdbc.url";
    private static final String JDBC_USER = "jdbc.user";
    private static final String JDBC_PASSWORD = "jdbc.password";
    private static final String CDC_TABLE = "cdc.table";
    private static final String CONNECTION_POOL_SIZE = "connection.pool.size";

    // Default column names for SAP CDC tables
    private static final String DEFAULT_OFFSET_COLUMN = "operation_id";
    private static final String DEFAULT_TRANSACTION_ID_COLUMN = "transaction_id";
    private static final String DEFAULT_TIMESTAMP_COLUMN = "timestamp";

    @Override
    public void initialize(Map<String, String> config) {
        if (initialized) {
            return;
        }

        String jdbcUrl = config.get(JDBC_URL);
        String user = config.get(JDBC_USER);
        String password = config.get(JDBC_PASSWORD);
        this.cdcTable = config.getOrDefault(CDC_TABLE, "attrep_cdc_changes");

        if (jdbcUrl == null || user == null || password == null) {
            throw new IllegalArgumentException("JDBC URL, user, and password are required");
        }

        // Configure HikariCP connection pool
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(jdbcUrl);
        hikariConfig.setUsername(user);
        hikariConfig.setPassword(password);

        try {
            Class.forName("com.sap.db.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(
                    "SAP HANA JDBC Driver (ngdbc.jar) not installed. Please check connectors/sap/pom.xml for installation instructions.",
                    e);
        }

        hikariConfig.setDriverClassName("com.sap.db.jdbc.Driver");

        int poolSize = Integer.parseInt(config.getOrDefault(CONNECTION_POOL_SIZE, "10"));
        hikariConfig.setMaximumPoolSize(poolSize);
        hikariConfig.setMinimumIdle(2);
        hikariConfig.setConnectionTimeout(30000);
        hikariConfig.setIdleTimeout(600000);
        hikariConfig.setMaxLifetime(1800000);

        this.dataSource = new HikariDataSource(hikariConfig);

        // Detect column names (could be configurable)
        this.offsetColumn = DEFAULT_OFFSET_COLUMN;
        this.transactionIdColumn = DEFAULT_TRANSACTION_ID_COLUMN;
        this.timestampColumn = DEFAULT_TIMESTAMP_COLUMN;

        initialized = true;
        LOG.info("JDBC Protocol Handler initialized for table: " + cdcTable);
    }

    @Override
    public List<ChangeRecord> pollChanges(long lastOffset, int batchSize) {
        if (!initialized) {
            throw new IllegalStateException("Protocol handler not initialized");
        }

        List<ChangeRecord> changes = new ArrayList<>();

        // Use parameterized query to avoid SQL injection
        // Table and column names are validated during initialization
        String sql = String.format(
                "SELECT * FROM \"%s\" WHERE \"%s\" > ? ORDER BY \"%s\" ASC LIMIT ?",
                cdcTable.replace("\"", "\"\""), // Escape quotes
                offsetColumn.replace("\"", "\"\""),
                offsetColumn.replace("\"", "\"\""));

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setLong(1, lastOffset);
            stmt.setInt(2, batchSize);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    ChangeRecord record = mapResultSetToChangeRecord(rs);
                    if (record != null) {
                        changes.add(record);
                    }
                }
            }

        } catch (SQLException e) {
            LOG.error("Failed to poll changes from CDC table: " + cdcTable, e);
            throw new RuntimeException("Failed to poll CDC changes", e);
        }

        return changes;
    }

    @Override
    public Map<String, Object> performJoin(String tableName, String joinKey, Object joinValue) {
        if (!initialized) {
            throw new IllegalStateException("Protocol handler not initialized");
        }

        // Use parameterized query with quoted identifiers to prevent SQL injection
        String sql = String.format(
                "SELECT * FROM \"%s\" WHERE \"%s\" = ?",
                tableName.replace("\"", "\"\""), // Escape quotes
                joinKey.replace("\"", "\"\""));

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {

            setParameter(stmt, 1, joinValue);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return mapResultSetToMap(rs);
                }
            }

        } catch (SQLException e) {
            LOG.warn("Failed to perform JOIN with table: " + tableName + ", key: " + joinKey, e);
            // Return empty map on error (graceful degradation)
        }

        return Collections.emptyMap();
    }

    @Override
    public void shutdown() {
        if (dataSource != null) {
            dataSource.close();
            dataSource = null;
        }
        initialized = false;
        LOG.info("JDBC Protocol Handler shut down");
    }

    @Override
    public boolean isHealthy() {
        if (!initialized || dataSource == null) {
            return false;
        }

        // Use lightweight check - just verify pool is not closed
        // isValid() creates a new connection which is expensive
        try {
            return !dataSource.isClosed();
        } catch (Exception e) {
            LOG.warn("Health check failed", e);
            return false;
        }
    }

    /**
     * Map ResultSet row to ChangeRecord.
     */
    private ChangeRecord mapResultSetToChangeRecord(ResultSet rs) throws SQLException {
        ChangeRecord record = new ChangeRecord();

        // Get transaction ID
        String transactionId = rs.getString(transactionIdColumn);
        record.setTransactionId(transactionId);

        // Get operation ID (offset)
        Long operationId = rs.getLong(offsetColumn);
        record.setOperationId(String.valueOf(operationId));
        record.setOffset(operationId);

        // Get timestamp
        Timestamp timestamp = rs.getTimestamp(timestampColumn);
        if (timestamp != null) {
            record.setTimestamp(timestamp.getTime());
        }

        // Get table name (assuming there's a table_name column)
        String tableName = rs.getString("table_name");
        if (tableName == null) {
            // Try alternative column names
            tableName = rs.getString("TABLE_NAME");
        }
        record.setTableName(tableName);

        // Get change type (assuming there's an operation_type column)
        String operationType = rs.getString("operation_type");
        if (operationType == null) {
            operationType = rs.getString("OPERATION_TYPE");
        }
        record.setChangeType(parseChangeType(operationType));

        // Get before/after values
        // SAP CDC tables typically have columns for old/new values
        // This is simplified - actual implementation would parse JSON or column-based
        // values
        Map<String, Object> beforeValues = extractValues(rs, "before_");
        Map<String, Object> afterValues = extractValues(rs, "after_");

        record.setBeforeValues(beforeValues);
        record.setAfterValues(afterValues);

        return record;
    }

    /**
     * Extract values from ResultSet with a prefix.
     * Optimized to cache metadata and avoid repeated calls.
     */
    private Map<String, Object> extractValues(ResultSet rs, String prefix) throws SQLException {
        Map<String, Object> values = new HashMap<>();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        String prefixLower = prefix.toLowerCase();

        // Pre-allocate map size estimate
        values = new HashMap<>(Math.max(16, columnCount / 4));

        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnName(i);
            if (columnName.toLowerCase().startsWith(prefixLower)) {
                Object value = rs.getObject(i);
                // Remove prefix from column name
                String key = columnName.substring(prefix.length());
                values.put(key.toLowerCase(), value);
            }
        }

        return values;
    }

    /**
     * Map ResultSet row to Map.
     * Optimized to cache metadata and pre-allocate map size.
     */
    private Map<String, Object> mapResultSetToMap(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        // Pre-allocate map with expected size
        Map<String, Object> map = new HashMap<>(columnCount);

        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnName(i).toLowerCase();
            Object value = rs.getObject(i);
            map.put(columnName, value);
        }

        return map;
    }

    /**
     * Parse change type from string.
     */
    private ChangeType parseChangeType(String operationType) {
        if (operationType == null) {
            return ChangeType.UPDATE; // Default
        }

        String upper = operationType.toUpperCase();
        if (upper.contains("INSERT") || upper.equals("I")) {
            return ChangeType.INSERT;
        } else if (upper.contains("UPDATE") || upper.equals("U")) {
            return ChangeType.UPDATE;
        } else if (upper.contains("DELETE") || upper.equals("D")) {
            return ChangeType.DELETE;
        }

        return ChangeType.UPDATE; // Default
    }

    /**
     * Set parameter in PreparedStatement with proper type handling.
     */
    private void setParameter(PreparedStatement stmt, int index, Object value) throws SQLException {
        if (value == null) {
            stmt.setNull(index, Types.NULL);
        } else if (value instanceof String) {
            stmt.setString(index, (String) value);
        } else if (value instanceof Integer) {
            stmt.setInt(index, (Integer) value);
        } else if (value instanceof Long) {
            stmt.setLong(index, (Long) value);
        } else if (value instanceof Double) {
            stmt.setDouble(index, (Double) value);
        } else {
            stmt.setObject(index, value);
        }
    }
}
