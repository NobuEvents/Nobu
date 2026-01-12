package com.nobu.queue;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.nobu.spi.event.NobuEvent;
import io.quarkus.logging.Log;
import org.eclipse.microprofile.config.ConfigProvider;

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
  
  // Default to 8192 (8K) - good balance for most use cases
  private static final int DEFAULT_BUFFER_SIZE = 8192;

  public EventQueue(String name) {
    this(name, getBufferSize());
  }
  
  public EventQueue(String name, int bufferSize) {
    // Ensure power of 2
    int actualSize = roundUpToPowerOfTwo(bufferSize);
    WaitStrategy waitStrategy = getWaitStrategy();
    
    this.eventDisruptor = new Disruptor<>(
        NobuEvent::new,
        actualSize,
        getThreadFactory(name),
        ProducerType.MULTI,  // Multiple producers (HTTP threads)
        waitStrategy
    );
    this.eventHandlerList = new ArrayList<>();
    Log.info("Disruptor Initialized for type: " + name + 
        " with buffer size: " + actualSize + 
        " and wait strategy: " + waitStrategy.getClass().getSimpleName());
  }
  
  private static int getBufferSize() {
    var config = ConfigProvider.getConfig();
    return config.getOptionalValue("disruptor.buffer.size", Integer.class)
        .orElse(DEFAULT_BUFFER_SIZE);
  }
  
  private static WaitStrategy getWaitStrategy() {
    var config = ConfigProvider.getConfig();
    String strategy = config.getOptionalValue(
        "disruptor.wait.strategy", String.class)
        .orElse("blocking");
    
    return switch (strategy.toLowerCase()) {
        case "busyspin" -> new BusySpinWaitStrategy();  // Lowest latency, highest CPU
        case "yielding" -> new YieldingWaitStrategy();  // Good balance
        case "sleeping" -> new SleepingWaitStrategy();   // Lower CPU, higher latency
        default -> new BlockingWaitStrategy();           // Default - good for most cases
    };
  }
  
  private static int roundUpToPowerOfTwo(int value) {
    // Disruptor requires power of 2
    int power = 1;
    while (power < value) {
        power <<= 1;
    }
    return power;
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

  @SuppressWarnings("unchecked")
  public void start() {
    EventHandler<NobuEvent>[] handler = new EventHandler[eventHandlerList.size()];
    eventDisruptor.handleEventsWith(eventHandlerList.toArray(handler));
    eventDisruptor.start();
  }
}
