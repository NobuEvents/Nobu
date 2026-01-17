package com.nobu.connect.sap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.nobu.queue.EventQueue;
import com.nobu.queue.EventQueueFactory;
import com.nobu.spi.event.NobuEvent;
import com.nobu.connect.sap.model.ChangeRecord;
import com.nobu.connect.sap.model.ChangeType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class SapEventInjectorTest {

    private EventQueueFactory mockEventQueueFactory;
    private EventQueue mockEventQueue;
    // private RingBuffer<NobuEvent> mockRingBuffer;
    private SapEventInjector.RingBufferOps mockRingBufferOps;
    private ObjectMapper objectMapper;
    private SapEventInjector eventInjector;
    
    /**
     * Create an InsufficientCapacityException instance using reflection
     * since the constructor is private.
     */
    private InsufficientCapacityException createInsufficientCapacityException() {
        try {
            Constructor<InsufficientCapacityException> constructor = 
                InsufficientCapacityException.class.getDeclaredConstructor();
            constructor.setAccessible(true);
            return constructor.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create InsufficientCapacityException", e);
        }
    }

    @BeforeEach
    public void setUp() {
        mockEventQueueFactory = mock(EventQueueFactory.class);
        mockEventQueue = mock(EventQueue.class);
        // mockRingBuffer = mock(RingBuffer.class); // Cannot mock final class
        mockRingBufferOps = mock(SapEventInjector.RingBufferOps.class);
        objectMapper = new ObjectMapper();
        
        // Use factory that returns our mock ops
        SapEventInjector.RingBufferOpsFactory opsFactory = rb -> mockRingBufferOps;
        eventInjector = new SapEventInjector(mockEventQueueFactory, objectMapper, 1000L, opsFactory);

        // Return null for RingBuffer since we use our own ops wrapper and ignore the real buffer
        when(mockEventQueue.getRingBuffer()).thenReturn(null);
    }

    @Test
    public void testInjectEventSuccess() throws Exception {
        String eventName = "test_event";
        ChangeRecord changeRecord = createTestChangeRecord();
        
        when(mockEventQueueFactory.get(eventName)).thenReturn(mockEventQueue);
        when(mockRingBufferOps.tryNext()).thenReturn(1L);
        
        NobuEvent mockEvent = mock(NobuEvent.class);
        when(mockRingBufferOps.get(1L)).thenReturn(mockEvent);

        assertDoesNotThrow(() -> {
            eventInjector.injectEvent(changeRecord, eventName, "srn:test", "host1");
        });

        verify(mockRingBufferOps).tryNext();
        verify(mockRingBufferOps).get(1L);
        verify(mockEvent).setEventName(eventName);
        verify(mockEvent).setSrn("srn:test");
        verify(mockEvent).setHost("host1");
        verify(mockEvent).setTimestamp(changeRecord.getTimestamp());
        verify(mockEvent).setEventId(changeRecord.getOperationId());
        verify(mockEvent).setMessage(any(byte[].class));
        verify(mockRingBufferOps).publish(1L);
    }

    @Test
    public void testInjectEventWithNullEventQueue() throws InsufficientCapacityException {
        String eventName = "nonexistent_event";
        ChangeRecord changeRecord = createTestChangeRecord();
        
        when(mockEventQueueFactory.get(eventName)).thenReturn(null);

        // Should not throw, just log warning
        assertDoesNotThrow(() -> {
            eventInjector.injectEvent(changeRecord, eventName, "srn:test", "host1");
        });

        verify(mockRingBufferOps, never()).tryNext();
    }

    @Test
    public void testInjectEventWithBackpressure() throws Exception, InsufficientCapacityException {
        String eventName = "test_event";
        ChangeRecord changeRecord = createTestChangeRecord();
        
        when(mockEventQueueFactory.get(eventName)).thenReturn(mockEventQueue);
        
        // Simulate backpressure - first call throws, second succeeds
        doThrow(createInsufficientCapacityException())
            .doReturn(1L)
            .when(mockRingBufferOps).tryNext();
        
        NobuEvent mockEvent = mock(NobuEvent.class);
        when(mockRingBufferOps.get(1L)).thenReturn(mockEvent);

        assertDoesNotThrow(() -> {
            eventInjector.injectEvent(changeRecord, eventName, "srn:test", "host1");
        });

        verify(mockRingBufferOps, times(2)).tryNext();
        verify(mockRingBufferOps).publish(1L);
    }

    @Test
    public void testInjectEventWithBackpressureTimeout() throws InsufficientCapacityException {
        String eventName = "test_event";
        ChangeRecord changeRecord = createTestChangeRecord();
        
        // Create injector with very short timeout and mock ops factory
        SapEventInjector.RingBufferOpsFactory opsFactory = rb -> mockRingBufferOps;
        SapEventInjector shortTimeoutInjector = new SapEventInjector(
            mockEventQueueFactory, objectMapper, 50L, opsFactory
        );
        
        when(mockEventQueueFactory.get(eventName)).thenReturn(mockEventQueue);
        doThrow(createInsufficientCapacityException())
            .when(mockRingBufferOps).tryNext();

        assertThrows(RuntimeException.class, () -> {
            shortTimeoutInjector.injectEvent(changeRecord, eventName, "srn:test", "host1");
        });
    }

    @Test
    public void testInjectEventWithDeleteOperation() throws Exception {
        String eventName = "test_event";
        ChangeRecord changeRecord = createTestChangeRecord();
        changeRecord.setChangeType(ChangeType.DELETE);
        changeRecord.setAfterValues(null);
        
        Map<String, Object> beforeValues = new HashMap<>();
        beforeValues.put("id", "123");
        changeRecord.setBeforeValues(beforeValues);
        
        when(mockEventQueueFactory.get(eventName)).thenReturn(mockEventQueue);
        when(mockRingBufferOps.tryNext()).thenReturn(1L);
        
        NobuEvent mockEvent = mock(NobuEvent.class);
        when(mockRingBufferOps.get(1L)).thenReturn(mockEvent);

        eventInjector.injectEvent(changeRecord, eventName, "srn:test", "host1");

        // Should use beforeValues for DELETE
        verify(mockEvent).setMessage(any(byte[].class));
    }

    @Test
    public void testInjectEventWithNullMessageData() throws Exception {
        String eventName = "test_event";
        ChangeRecord changeRecord = createTestChangeRecord();
        changeRecord.setAfterValues(null);
        changeRecord.setBeforeValues(null);
        
        when(mockEventQueueFactory.get(eventName)).thenReturn(mockEventQueue);
        when(mockRingBufferOps.tryNext()).thenReturn(1L);
        
        NobuEvent mockEvent = mock(NobuEvent.class);
        when(mockRingBufferOps.get(1L)).thenReturn(mockEvent);

        eventInjector.injectEvent(changeRecord, eventName, "srn:test", "host1");

        verify(mockEvent).setMessage(new byte[0]);
    }

    @Test
    public void testInjectEventInterrupted() throws InsufficientCapacityException {
        String eventName = "test_event";
        ChangeRecord changeRecord = createTestChangeRecord();
        
        when(mockEventQueueFactory.get(eventName)).thenReturn(mockEventQueue);
        doThrow(createInsufficientCapacityException())
            .when(mockRingBufferOps).tryNext();

        Thread.currentThread().interrupt();

        assertThrows(RuntimeException.class, () -> {
            eventInjector.injectEvent(changeRecord, eventName, "srn:test", "host1");
        });

        assertTrue(Thread.currentThread().isInterrupted());
    }

    @Test
    public void testInjectEventWithJsonSerializationError() throws Exception {
        String eventName = "test_event";
        ChangeRecord changeRecord = createTestChangeRecord();
        
        // Create a change record with non-serializable data
        Map<String, Object> afterValues = new HashMap<>();
        afterValues.put("circular", new Object() {
            @Override
            public String toString() {
                return "circular";
            }
        });
        changeRecord.setAfterValues(afterValues);
        
        when(mockEventQueueFactory.get(eventName)).thenReturn(mockEventQueue);
        when(mockRingBufferOps.tryNext()).thenReturn(1L);
        
        NobuEvent mockEvent = mock(NobuEvent.class);
        when(mockRingBufferOps.get(1L)).thenReturn(mockEvent);

        // Should handle serialization gracefully
        assertDoesNotThrow(() -> {
            eventInjector.injectEvent(changeRecord, eventName, "srn:test", "host1");
        });
    }

    private ChangeRecord createTestChangeRecord() {
        ChangeRecord record = new ChangeRecord();
        record.setTransactionId("txn1");
        record.setTableName("test_table");
        record.setChangeType(ChangeType.INSERT);
        record.setOperationId("op1");
        record.setOffset(1L);
        record.setTimestamp(System.currentTimeMillis());
        
        Map<String, Object> afterValues = new HashMap<>();
        afterValues.put("id", "123");
        afterValues.put("name", "test");
        record.setAfterValues(afterValues);
        
        return record;
    }
}
