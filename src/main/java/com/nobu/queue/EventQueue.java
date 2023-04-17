package com.nobu.queue;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.nobu.event.NobuEvent;
import io.quarkus.logging.Log;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;


/***
 * LMax Disruptor Broadcasting Queue implementation for dynamic event publishing
 * See: <a href="https://github.com/lmax-io/disruptor/blob/master">LMax Disruptor</a>
 */
public class EventQueue {

  private final Disruptor<NobuEvent> eventDisruptor;
  private final List<EventHandler<NobuEvent>> eventHandlerList;
  private static final int DEFAULT_BUFFER_SIZE = 1024;

  public EventQueue(String name) {
    this.eventDisruptor = new Disruptor<>(NobuEvent::new, DEFAULT_BUFFER_SIZE, getThreadFactory(name));
    this.eventHandlerList = new ArrayList<>();
    Log.info("Disruptor Initialized for the type:" + name);
  }

    /*private ThreadFactory getVirtualThreadFactory(String name) {
        return Thread
                //.ofVirtual()
                .name(name)
                .uncaughtExceptionHandler(new ConsumerUncaughtExceptionHandler())
                .factory();
    }*/

  private ThreadFactory getThreadFactory(String name) {
    return r -> {
      Thread t = new Thread(r);
      t.setUncaughtExceptionHandler(new ConsumerUncaughtExceptionHandler());
      t.setName(name);
      return t;
    };
  }

  public void addHandle(final EventHandler<NobuEvent> handler) {
    eventHandlerList.add(handler);
  }

  public RingBuffer<NobuEvent> getRingBuffer() {
    return eventDisruptor.getRingBuffer();
  }

  public void start() {
    EventHandler<NobuEvent>[] handler = new EventHandler[eventHandlerList.size()];
    eventDisruptor.handleEventsWith(eventHandlerList.toArray(handler));
    eventDisruptor.start();
  }
}
