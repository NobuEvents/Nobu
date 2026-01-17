package com.nobu.connect.sap.jdbc;

import com.nobu.connect.sap.model.ChangeRecord;
import com.nobu.connect.sap.model.ChangeType;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class JdbcProtocolHandlerTest {

    @Mock
    private HikariDataSource mockDataSource;

    @Mock
    private Connection mockConnection;

    @Mock
    private PreparedStatement mockPreparedStatement;

    @Mock
    private ResultSet mockResultSet;

    @Mock
    private ResultSetMetaData mockMetaData;

    private JdbcProtocolHandler protocolHandler;
    private Map<String, String> config;

    @BeforeEach
    public void setUp() throws Exception {
        protocolHandler = new JdbcProtocolHandler();
        
        // Inject mock datasource using reflection
        Field dataSourceField = JdbcProtocolHandler.class.getDeclaredField("dataSource");
        dataSourceField.setAccessible(true);
        dataSourceField.set(protocolHandler, mockDataSource);
        
        config = new HashMap<>();
        config.put("jdbc.url", "jdbc:sap://localhost:30015");
        config.put("jdbc.user", "testuser");
        config.put("jdbc.password", "testpass");
        config.put("cdc.table", "test_cdc_table");
        
        when(mockDataSource.getConnection()).thenReturn(mockConnection);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    }

    @Test
    public void testInitialize() {
        assertDoesNotThrow(() -> protocolHandler.initialize(config));
        assertTrue(protocolHandler.isHealthy());
    }

    @Test
    public void testInitializeMissingRequiredConfig() {
        config.remove("jdbc.url");
        
        assertThrows(IllegalArgumentException.class, () -> {
            protocolHandler.initialize(config);
        });
    }

    @Test
    public void testPollChanges() throws SQLException {
        protocolHandler.initialize(config);
        
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
        when(mockResultSet.next())
            .thenReturn(true)
            .thenReturn(false);
        when(mockResultSet.getMetaData()).thenReturn(mockMetaData);
        when(mockMetaData.getColumnCount()).thenReturn(5);
        when(mockMetaData.getColumnName(anyInt())).thenReturn("column" + anyInt());
        
        // Mock result set values
        when(mockResultSet.getString("transaction_id")).thenReturn("txn1");
        when(mockResultSet.getLong("operation_id")).thenReturn(100L);
        when(mockResultSet.getTimestamp("timestamp")).thenReturn(new Timestamp(System.currentTimeMillis()));
        when(mockResultSet.getString("table_name")).thenReturn("test_table");
        when(mockResultSet.getString("operation_type")).thenReturn("INSERT");

        List<ChangeRecord> changes = protocolHandler.pollChanges(0L, 100);

        assertNotNull(changes);
        verify(mockPreparedStatement).setLong(1, 0L);
        verify(mockPreparedStatement).setInt(2, 100);
    }

    @Test
    public void testPollChangesWithNoResults() throws SQLException {
        protocolHandler.initialize(config);
        
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
        when(mockResultSet.next()).thenReturn(false);

        List<ChangeRecord> changes = protocolHandler.pollChanges(0L, 100);

        assertTrue(changes.isEmpty());
    }

    @Test
    public void testPollChangesWithSQLException() throws SQLException {
        protocolHandler.initialize(config);
        
        when(mockPreparedStatement.executeQuery()).thenThrow(new SQLException("Database error"));

        assertThrows(RuntimeException.class, () -> {
            protocolHandler.pollChanges(0L, 100);
        });
    }

    @Test
    public void testPerformJoin() throws SQLException {
        protocolHandler.initialize(config);
        
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
        when(mockResultSet.next())
            .thenReturn(true)
            .thenReturn(false);
        when(mockResultSet.getMetaData()).thenReturn(mockMetaData);
        when(mockMetaData.getColumnCount()).thenReturn(2);
        when(mockMetaData.getColumnName(1)).thenReturn("id");
        when(mockMetaData.getColumnName(2)).thenReturn("name");
        when(mockResultSet.getObject(1)).thenReturn("123");
        when(mockResultSet.getObject(2)).thenReturn("Test Name");

        Map<String, Object> result = protocolHandler.performJoin("lookup_table", "id", "123");

        assertNotNull(result);
        assertFalse(result.isEmpty());
        verify(mockPreparedStatement).setObject(1, "123");
    }

    @Test
    public void testPerformJoinWithNoResults() throws SQLException {
        protocolHandler.initialize(config);
        
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
        when(mockResultSet.next()).thenReturn(false);

        Map<String, Object> result = protocolHandler.performJoin("lookup_table", "id", "123");

        assertTrue(result.isEmpty());
    }

    @Test
    public void testPerformJoinWithSQLException() throws SQLException {
        protocolHandler.initialize(config);
        
        when(mockPreparedStatement.executeQuery()).thenThrow(new SQLException("Database error"));

        Map<String, Object> result = protocolHandler.performJoin("lookup_table", "id", "123");

        assertTrue(result.isEmpty()); // Graceful degradation
    }

    @Test
    public void testIsHealthy() throws SQLException {
        protocolHandler.initialize(config);
        
        assertTrue(protocolHandler.isHealthy());
        
        when(mockDataSource.isClosed()).thenReturn(true);
        assertFalse(protocolHandler.isHealthy());
    }

    @Test
    public void testIsHealthyNotInitialized() {
        assertFalse(protocolHandler.isHealthy());
    }

    @Test
    public void testShutdown() {
        protocolHandler.initialize(config);
        
        assertDoesNotThrow(() -> protocolHandler.shutdown());
        assertFalse(protocolHandler.isHealthy());
    }

    @Test
    public void testPollChangesNotInitialized() {
        assertThrows(IllegalStateException.class, () -> {
            protocolHandler.pollChanges(0L, 100);
        });
    }

    @Test
    public void testPerformJoinNotInitialized() {
        assertThrows(IllegalStateException.class, () -> {
            protocolHandler.performJoin("table", "key", "value");
        });
    }

    @Test
    public void testPollChangesWithDifferentChangeTypes() throws SQLException {
        protocolHandler.initialize(config);
        
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
        when(mockResultSet.next())
            .thenReturn(true)
            .thenReturn(true)
            .thenReturn(true)
            .thenReturn(false);
        when(mockResultSet.getMetaData()).thenReturn(mockMetaData);
        when(mockMetaData.getColumnCount()).thenReturn(5);
        when(mockMetaData.getColumnName(anyInt())).thenReturn("column" + anyInt());
        
        when(mockResultSet.getString("transaction_id")).thenReturn("txn1");
        when(mockResultSet.getLong("operation_id")).thenReturn(100L);
        when(mockResultSet.getTimestamp("timestamp")).thenReturn(new Timestamp(System.currentTimeMillis()));
        when(mockResultSet.getString("table_name")).thenReturn("test_table");
        when(mockResultSet.getString("operation_type"))
            .thenReturn("INSERT")
            .thenReturn("UPDATE")
            .thenReturn("DELETE");

        List<ChangeRecord> changes = protocolHandler.pollChanges(0L, 100);

        assertEquals(3, changes.size());
        assertEquals(ChangeType.INSERT, changes.get(0).getChangeType());
        assertEquals(ChangeType.UPDATE, changes.get(1).getChangeType());
        assertEquals(ChangeType.DELETE, changes.get(2).getChangeType());
    }

    @Test
    public void testPerformJoinWithDifferentParameterTypes() throws SQLException {
        protocolHandler.initialize(config);
        
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
        when(mockResultSet.next()).thenReturn(false);
        when(mockResultSet.getMetaData()).thenReturn(mockMetaData);
        when(mockMetaData.getColumnCount()).thenReturn(0);
        
        // Test with different join value types
        protocolHandler.performJoin("lookup_table", "id", 123);
        protocolHandler.performJoin("lookup_table", "id", 123L);
        protocolHandler.performJoin("lookup_table", "id", "string");
        
        verify(mockPreparedStatement, times(3)).executeQuery();
    }
}
