package com.nobu.server;

import com.lmax.disruptor.RingBuffer;
import com.nobu.cel.CelValidator;
import com.nobu.event.NobuEvent;
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
import org.jboss.logging.Logger;


@Path("/event")
@QuarkusMain
@ApplicationScoped
public class NobuServer {

  private static final Logger LOG = Logger.getLogger(NobuServer.class);

  @Inject
  EventQueueFactory eventQueueFactory;

  @Inject
  CelValidator celValidator;

  public static final String DLQ_TYPE_NAME = "dlq";

  @PostConstruct
  public void init() {
    LOG.info("EventQueue created");
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public String event(NobuEvent event) {

    EventQueue eventQueue = getEventQueue(event);

    if (eventQueue == null) {
      LOG.error("No EventQueue queue for type " + event.getType());
      return "error";
    }

    RingBuffer<NobuEvent> ringBuffer = eventQueue.getRingBuffer();
    long sequence = ringBuffer.next();
    try {
      NobuEvent nobuEvent = ringBuffer.get(sequence);
      nobuEvent.deepCopy(event);
    } finally {
      ringBuffer.publish(sequence);
    }
    return "ok";
  }

  private EventQueue getEventQueue(NobuEvent event) {
    if (!celValidator.test(event)) {
      LOG.warn("CEL validation failed for event type " + event.getType() +
          " and event schema " + event.getSchema());
      return eventQueueFactory.get(DLQ_TYPE_NAME);
    } else {
      return eventQueueFactory.get(event.getType());
    }
  }

  public static void main(String... args) {
    LOG.info("Running main method");
    Quarkus.run(args);
  }
}
