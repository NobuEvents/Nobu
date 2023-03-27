package com.nobu.server;

import com.lmax.disruptor.RingBuffer;
import com.nobu.cel.CelValidator;
import com.nobu.event.NobuEvent;
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

    @PostConstruct
    public void init() {
        LOG.info("DisruptorQueue created");
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public String event(NobuEvent event) {
        LOG.info(event);

        if(!celValidator.test(event)) {
            LOG.error("CEL validation failed");
            return "error";
        }

        var disruptorQueue = disruptorQueueFactory.get(event.getType());
        if (disruptorQueue == null) {
            LOG.error("No disruptor queue for type " + event.getType());
            return "error";
        }
        RingBuffer<NobuEvent> ringBuffer = disruptorQueue.getRingBuffer();
        long sequence = ringBuffer.next();
        NobuEvent nobuEvent = ringBuffer.get(sequence);
        nobuEvent.deepCopy(event);
        // Run data quality checks


        ringBuffer.publish(sequence);
        return "ok";
    }

    public static void main(String... args) {
        LOG.info("Running main method");
        Quarkus.run(args);
    }
}
