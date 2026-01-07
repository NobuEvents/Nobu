package com.nobu.queue;

import com.lmax.disruptor.EventHandler;
import com.nobu.cel.CelValidator;
import com.nobu.spi.event.NobuEvent;
import org.jboss.logging.Logger;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class ValidationEventHandler implements EventHandler<NobuEvent> {
    private static final Logger LOG = Logger.getLogger(ValidationEventHandler.class);
    
    @Inject
    CelValidator celValidator;
    
    @Inject
    EventQueueFactory eventQueueFactory;
    
    private static final String DLQ_TYPE_NAME = "dlq";
    
    @Override
    public void onEvent(NobuEvent event, long sequence, boolean endOfBatch) {
        // Validation happens asynchronously in Disruptor handler
        if (event.getSrn() != null && !celValidator.test(event)) {
            LOG.warn("CEL validation failed for event type " + event.getEventName() +
                " and event schema " + event.getSrn());
            
            // Route to DLQ
            EventQueue dlqQueue = eventQueueFactory.get(DLQ_TYPE_NAME);
            if (dlqQueue != null) {
                var ringBuffer = dlqQueue.getRingBuffer();
                long dlqSequence = ringBuffer.next();
                try {
                    NobuEvent dlqEvent = ringBuffer.get(dlqSequence);
                    dlqEvent.deepCopy(event);
                } finally {
                    ringBuffer.publish(dlqSequence);
                }
            }
        }
    }
}

