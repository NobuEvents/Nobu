package com.nobu.server;

import com.lmax.disruptor.RingBuffer;
import com.nobu.cel.CelValidator;
import com.nobu.event.NobuEvent;
import com.nobu.queue.DisruptorQueue;
import com.nobu.queue.DisruptorQueueFactory;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.annotations.QuarkusMain;
import org.jboss.logging.Logger;


import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;


@Path("/event")
@QuarkusMain
@ApplicationScoped
public class Server {

    private static final Logger LOG = Logger.getLogger(Server.class);

    @Inject
    DisruptorQueueFactory disruptorQueueFactory;

    @Inject
    CelValidator celValidator;

    public static final String DLQ_TYPE_NAME = "dlq";

    @PostConstruct
    public void init() {
        LOG.info("DisruptorQueue created");
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public String event(NobuEvent event) {

        DisruptorQueue disruptorQueue = getDisruptorQueueForEvent(event);

        if (disruptorQueue == null) {
            LOG.error("No disruptor queue for type " + event.getType());
            return "error";
        }

        RingBuffer<NobuEvent> ringBuffer = disruptorQueue.getRingBuffer();
        long sequence = ringBuffer.next();
        try {
            NobuEvent nobuEvent = ringBuffer.get(sequence);
            nobuEvent.deepCopy(event);
        } finally {
            ringBuffer.publish(sequence);
        }
        return "ok";
    }

    private DisruptorQueue getDisruptorQueueForEvent(NobuEvent event) {
        if (!celValidator.test(event)) {
            LOG.warn("CEL validation failed for event type " + event.getType() +
                    " and event schema " + event.getSchema());
            return disruptorQueueFactory.get(DLQ_TYPE_NAME);
        } else {
            return disruptorQueueFactory.get(event.getType());
        }
    }


    public static void main(String... args) {
        LOG.info("Running main method");
        Quarkus.run(args);
    }
}
