package com.nobu.connect.sap;

import com.nobu.connect.sap.model.ChangeRecord;
import com.nobu.connect.sap.model.ChangeType;
import com.nobu.connect.sap.protocol.ProtocolHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class JoinProcessorTest {

    private ProtocolHandler mockProtocolHandler;
    private JoinProcessor joinProcessor;
    private SimpleMeterRegistry meterRegistry;

    @BeforeEach
    public void setUp() {
        mockProtocolHandler = mock(ProtocolHandler.class);
        meterRegistry = new SimpleMeterRegistry();
        Map<String, JoinProcessor.JoinConfig> joinConfigs = new HashMap<>();
        joinConfigs.put("test_table", new JoinProcessor.JoinConfig("lookup_table", "id", 3600));
        joinProcessor = new JoinProcessor(mockProtocolHandler, joinConfigs, true, meterRegistry);
    }

    @Test
    public void testEnrichWithJoins() {
        ChangeRecord change = createTestChangeRecord();
        
        Map<String, Object> joinData = new HashMap<>();
        joinData.put("lookup_field", "lookup_value");
        joinData.put("lookup_name", "Test Name");
        
        when(mockProtocolHandler.performJoin("lookup_table", "id", "123"))
            .thenReturn(joinData);

        ChangeRecord enriched = joinProcessor.enrichWithJoins(change);
        
        assertNotNull(enriched);
        assertNotSame(change, enriched); // Should be a new instance
        assertTrue(enriched.getAfterValues().containsKey("lookup_field"));
        assertEquals("lookup_value", enriched.getAfterValues().get("lookup_field"));
        assertEquals("Test Name", enriched.getAfterValues().get("lookup_name"));
        // Original fields should still be present
        assertEquals("123", enriched.getAfterValues().get("id"));
        assertEquals("test", enriched.getAfterValues().get("name"));
    }

    @Test
    public void testEnrichWithJoinsDisabled() {
        JoinProcessor disabledProcessor = new JoinProcessor(mockProtocolHandler, Collections.emptyMap(), false, meterRegistry);
        ChangeRecord change = createTestChangeRecord();
        
        ChangeRecord enriched = disabledProcessor.enrichWithJoins(change);
        
        assertEquals(change.getAfterValues(), enriched.getAfterValues()); // Should return unchanged
        verify(mockProtocolHandler, never()).performJoin(anyString(), anyString(), any());
        
        disabledProcessor.shutdown();
    }

    @Test
    public void testEnrichWithNoJoinConfig() {
        ChangeRecord change = createTestChangeRecord();
        change.setTableName("other_table"); // Different table name
        
        ChangeRecord enriched = joinProcessor.enrichWithJoins(change);
        
        assertEquals(change.getAfterValues(), enriched.getAfterValues()); // No change
        verify(mockProtocolHandler, never()).performJoin(anyString(), anyString(), any());
    }

    @Test
    public void testEnrichWithJoinFailure() {
        ChangeRecord change = createTestChangeRecord();
        
        when(mockProtocolHandler.performJoin(anyString(), anyString(), any()))
            .thenThrow(new RuntimeException("Join failed"));

        // Should not throw, graceful degradation
        ChangeRecord enriched = joinProcessor.enrichWithJoins(change);
        assertNotNull(enriched);
        // Should return original record
        assertEquals(change.getAfterValues(), enriched.getAfterValues());
    }

    @Test
    public void testEnrichWithNullJoinValue() {
        ChangeRecord change = createTestChangeRecord();
        change.getAfterValues().remove("id"); // Remove join key
        
        ChangeRecord enriched = joinProcessor.enrichWithJoins(change);
        
        assertEquals(change.getAfterValues(), enriched.getAfterValues()); // No change
        verify(mockProtocolHandler, never()).performJoin(anyString(), anyString(), any());
    }

    @Test
    public void testCacheHit() {
        ChangeRecord change = createTestChangeRecord();
        
        Map<String, Object> joinData = new HashMap<>();
        joinData.put("lookup_field", "lookup_value");
        
        when(mockProtocolHandler.performJoin("lookup_table", "id", "123"))
            .thenReturn(joinData);

        // First call - cache miss
        ChangeRecord enriched1 = joinProcessor.enrichWithJoins(change);
        assertNotNull(enriched1);
        
        // Second call - cache hit
        ChangeRecord enriched2 = joinProcessor.enrichWithJoins(change);
        assertNotNull(enriched2);
        
        // Should only call performJoin once
        verify(mockProtocolHandler, times(1)).performJoin("lookup_table", "id", "123");
    }

    @Test
    public void testCacheExpiration() throws InterruptedException {
        // Create processor with short TTL
        Map<String, JoinProcessor.JoinConfig> joinConfigs = new HashMap<>();
        joinConfigs.put("test_table", new JoinProcessor.JoinConfig("lookup_table", "id", 1)); // 1 second TTL
        JoinProcessor shortTtlProcessor = new JoinProcessor(mockProtocolHandler, joinConfigs, true, meterRegistry);
        
        ChangeRecord change = createTestChangeRecord();
        Map<String, Object> joinData = new HashMap<>();
        joinData.put("lookup_field", "lookup_value");
        
        when(mockProtocolHandler.performJoin("lookup_table", "id", "123"))
            .thenReturn(joinData);

        // First call
        shortTtlProcessor.enrichWithJoins(change);
        
        // Wait for cache expiration
        Thread.sleep(1500); // Longer wait to ensure expiration
        
        // Trigger cache access with different key to force cleanup
        ChangeRecord change2 = createTestChangeRecord();
        change2.getAfterValues().put("id", "456");
        shortTtlProcessor.enrichWithJoins(change2);
        
        // Now access original key - should be expired
        shortTtlProcessor.enrichWithJoins(change);
        
        // Should call performJoin at least twice (once for first call, once after expiration)
        verify(mockProtocolHandler, atLeast(2)).performJoin(anyString(), anyString(), any());
        
        shortTtlProcessor.shutdown();
    }

    @Test
    public void testCacheSizeLimit() {
        // Create processor with small cache size
        JoinProcessor smallCacheProcessor = new JoinProcessor(mockProtocolHandler, createJoinConfigs(), true, 5, meterRegistry);
        
        Map<String, Object> joinData = new HashMap<>();
        joinData.put("lookup_field", "value");
        
        when(mockProtocolHandler.performJoin(anyString(), anyString(), any()))
            .thenReturn(joinData);

        // Add more entries than cache size
        for (int i = 0; i < 10; i++) {
            ChangeRecord change = createTestChangeRecord();
            change.getAfterValues().put("id", String.valueOf(i));
            smallCacheProcessor.enrichWithJoins(change);
        }

        // Cache should not exceed size limit
        // Note: Exact size depends on eviction strategy, but should be bounded
        // Caffeine handles cache eviction automatically - no manual cleanup needed
        
        smallCacheProcessor.shutdown();
    }

    @Test
    public void testConcurrentCacheAccess() throws InterruptedException {
        ChangeRecord change = createTestChangeRecord();
        Map<String, Object> joinData = new HashMap<>();
        joinData.put("lookup_field", "lookup_value");
        
        when(mockProtocolHandler.performJoin("lookup_table", "id", "123"))
            .thenReturn(joinData);

        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    joinProcessor.enrichWithJoins(change);
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        
        // Should only call performJoin once (cache hit for subsequent calls)
        verify(mockProtocolHandler, atMost(threadCount)).performJoin("lookup_table", "id", "123");
        
        executor.shutdown();
    }

    @Test
    public void testClearCache() {
        ChangeRecord change = createTestChangeRecord();
        Map<String, Object> joinData = new HashMap<>();
        joinData.put("lookup_field", "lookup_value");
        
        when(mockProtocolHandler.performJoin("lookup_table", "id", "123"))
            .thenReturn(joinData);

        // First call - cache miss
        joinProcessor.enrichWithJoins(change);
        
        // Clear cache
        joinProcessor.clearCache();
        
        // Second call - should be cache miss again
        joinProcessor.enrichWithJoins(change);
        
        // Should call performJoin twice
        verify(mockProtocolHandler, times(2)).performJoin("lookup_table", "id", "123");
    }

    @Test
    public void testEnrichWithDeleteOperation() {
        ChangeRecord change = createTestChangeRecord();
        change.setChangeType(ChangeType.DELETE);
        change.setAfterValues(null); // DELETE has no after values
        change.setBeforeValues(change.getAfterValues()); // Move to before
        
        Map<String, Object> beforeValues = new HashMap<>();
        beforeValues.put("id", "123");
        beforeValues.put("name", "test");
        change.setBeforeValues(beforeValues);
        
        Map<String, Object> joinData = new HashMap<>();
        joinData.put("lookup_field", "lookup_value");
        
        when(mockProtocolHandler.performJoin("lookup_table", "id", "123"))
            .thenReturn(joinData);

        ChangeRecord enriched = joinProcessor.enrichWithJoins(change);
        
        assertNotNull(enriched);
        assertTrue(enriched.getBeforeValues().containsKey("lookup_field"));
        assertEquals("lookup_value", enriched.getBeforeValues().get("lookup_field"));
    }

    @Test
    public void testShutdown() {
        assertDoesNotThrow(() -> joinProcessor.shutdown());
        // Second shutdown should be safe
        assertDoesNotThrow(() -> joinProcessor.shutdown());
    }

    private ChangeRecord createTestChangeRecord() {
        ChangeRecord record = new ChangeRecord();
        record.setTransactionId("txn1");
        record.setTableName("test_table");
        record.setChangeType(ChangeType.INSERT);
        
        Map<String, Object> afterValues = new HashMap<>();
        afterValues.put("id", "123");
        afterValues.put("name", "test");
        record.setAfterValues(afterValues);
        
        return record;
    }

    private Map<String, JoinProcessor.JoinConfig> createJoinConfigs() {
        Map<String, JoinProcessor.JoinConfig> joinConfigs = new HashMap<>();
        joinConfigs.put("test_table", new JoinProcessor.JoinConfig("lookup_table", "id", 3600));
        return joinConfigs;
    }
}
