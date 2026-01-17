package com.nobu.connect.sap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.nobu.queue.EventQueueFactory;
import com.nobu.spi.event.NobuEvent;
import org.jboss.logging.Logger;

/**
 * Handles injection of SAP CDC events into Nobu's EventQueue system.
 * Converts ChangeRecords to NobuEvents and publishes them to the appropriate EventQueue.
 */
public class SapEventInjector {
    private static final Logger LOG = Logger.getLogger(SapEventInjector.class);
    
    private final EventQueueFactory eventQueueFactory;
    private final ObjectMapper objectMapper;
    private final long backpressureTimeoutMs;
    private final RingBufferOpsFactory ringBufferOpsFactory;

    public interface RingBufferOps {
        long tryNext() throws InsufficientCapacityException;
        NobuEvent get(long sequence);
        void publish(long sequence);
    }

    // Factory to create RingBufferOps for a given RingBuffer
    // Visible for testing
    interface RingBufferOpsFactory {
        RingBufferOps create(RingBuffer<NobuEvent> ringBuffer);
    }

    public SapEventInjector(EventQueueFactory eventQueueFactory, ObjectMapper objectMapper) {
        this(eventQueueFactory, objectMapper, 1000L); // Default 1 second timeout
    }

    public SapEventInjector(EventQueueFactory eventQueueFactory, ObjectMapper objectMapper, long backpressureTimeoutMs) {
        this.eventQueueFactory = eventQueueFactory;
        this.objectMapper = objectMapper;
        this.backpressureTimeoutMs = backpressureTimeoutMs;
        this.ringBufferOpsFactory = ringBuffer -> new RingBufferOps() {
            @Override
            public long tryNext() throws InsufficientCapacityException {
                return ringBuffer.tryNext();
            }

            @Override
            public NobuEvent get(long sequence) {
                return ringBuffer.get(sequence);
            }

            @Override
            public void publish(long sequence) {
                ringBuffer.publish(sequence);
            }
        };
    }

    // Constructor for testing
    protected SapEventInjector(EventQueueFactory eventQueueFactory, ObjectMapper objectMapper, 
                             long backpressureTimeoutMs, RingBufferOpsFactory ringBufferOpsFactory) {
        this.eventQueueFactory = eventQueueFactory;
        this.objectMapper = objectMapper;
        this.backpressureTimeoutMs = backpressureTimeoutMs;
        this.ringBufferOpsFactory = ringBufferOpsFactory;
    }

    /**
     * Inject a change record as a NobuEvent into the appropriate EventQueue.
     * 
     * @param changeRecord The change record to inject
     * @param eventName The event name (typically derived from table name)
     * @param srn Schema Resource Name
     * @param host Source host identifier
     */
    public void injectEvent(com.nobu.connect.sap.model.ChangeRecord changeRecord, 
                           String eventName, String srn, String host) {
        try {
            var eventQueue = eventQueueFactory.get(eventName);
            if (eventQueue == null) {
                LOG.warnf("No EventQueue found for event name: %s. Event will be dropped.", eventName);
                return;
            }

            RingBuffer<NobuEvent> ringBuffer = eventQueue.getRingBuffer();
            RingBufferOps ops = ringBufferOpsFactory.create(ringBuffer);
            long sequence = tryGetSequence(ops);

            try {
                NobuEvent nobuEvent = ops.get(sequence);
                
                // Set event properties
                nobuEvent.setEventName(eventName);
                nobuEvent.setSrn(srn);
                nobuEvent.setHost(host);
                nobuEvent.setTimestamp(changeRecord.getTimestamp());
                nobuEvent.setEventId(changeRecord.getOperationId());

                // Convert change record to JSON message
                // Use afterValues for INSERT/UPDATE, beforeValues for DELETE
                var messageData = changeRecord.getChangeType() == com.nobu.connect.sap.model.ChangeType.DELETE
                    ? changeRecord.getBeforeValues()
                    : changeRecord.getAfterValues();

                if (messageData != null) {
                    try {
                        byte[] messageBytes = objectMapper.writeValueAsBytes(messageData);
                        nobuEvent.setMessage(messageBytes);
                    } catch (Exception e) {
                        LOG.warnf("Failed to serialize message data for event %s. Sending empty message. Error: %s", 
                            eventName, e.getMessage());
                        nobuEvent.setMessage(new byte[0]);
                    }
                } else {
                    nobuEvent.setMessage(new byte[0]);
                }

            } finally {
                ops.publish(sequence);
            }

        } catch (Exception e) {
            LOG.errorf(e, "Failed to inject event for table %s: %s", changeRecord.getTableName(), e.getMessage());
            throw new RuntimeException("Failed to inject SAP CDC event", e);
        }
    }

    /**
     * Try to get a sequence from the ring buffer with backpressure handling.
     */
    private long tryGetSequence(RingBufferOps ops) {
        long startTime = System.currentTimeMillis();
        
        while (true) {
            try {
                return ops.tryNext();
            } catch (InsufficientCapacityException e) {
                // Buffer is full, check timeout
                if (System.currentTimeMillis() - startTime > backpressureTimeoutMs) {
                    throw new RuntimeException("EventQueue buffer full, timeout exceeded", e);
                }
                // Brief yield before retry
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for buffer capacity", ie);
                }
            }
        }
    }
}
