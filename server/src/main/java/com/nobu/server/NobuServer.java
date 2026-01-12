package com.nobu.server;

import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.nobu.spi.event.NobuEvent;
import com.nobu.queue.EventQueue;
import com.nobu.queue.EventQueueFactory;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.annotations.QuarkusMain;
import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.jboss.logging.Logger;


@Path("/event")
@QuarkusMain
@ApplicationScoped
public class NobuServer {

  private static final Logger LOG = Logger.getLogger(NobuServer.class);

  @Inject
  EventQueueFactory eventQueueFactory;

  public static final String DLQ_TYPE_NAME = "dlq";

  @PostConstruct
  public void init() {
    LOG.info("EventQueue created");
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response event(NobuEvent event) {
    EventQueue eventQueue = eventQueueFactory.get(event.getEventName());
    
    if (eventQueue == null) {
      LOG.error("No EventQueue queue for type " + event.getEventName());
      return Response.status(Response.Status.BAD_REQUEST)
          .entity("error").build();
    }
    
    RingBuffer<NobuEvent> ringBuffer = eventQueue.getRingBuffer();
    
    // Use tryNext() with timeout for backpressure handling
    long timeout = 100; // milliseconds
    long startTime = System.currentTimeMillis();
    long sequence;
    
    while (true) {
      try {
        sequence = ringBuffer.tryNext();
        break; // Successfully got sequence
      } catch (InsufficientCapacityException e) {
        // Buffer is full, check timeout
        if (System.currentTimeMillis() - startTime > timeout) {
          return Response.status(Response.Status.SERVICE_UNAVAILABLE)
              .entity("Service temporarily unavailable").build();
        }
        Thread.yield(); // Brief yield before retry
      }
    }
    
    try {
      NobuEvent nobuEvent = ringBuffer.get(sequence);
      nobuEvent.deepCopy(event);
    } finally {
      ringBuffer.publish(sequence);
    }
    
    return Response.ok("ok").build();
  }

  public static void main(String... args) {
    LOG.info("Running main method");
    Quarkus.run(args);
  }
}
