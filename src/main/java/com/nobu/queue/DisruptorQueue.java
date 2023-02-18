package com.nobu.queue;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.nobu.event.NobuEvent;
import io.quarkus.logging.Log;


import java.util.ArrayList;
import java.util.List;


public class DisruptorQueue {

    private final Disruptor<NobuEvent> eventDisruptor;
    private final List<EventHandler<NobuEvent>> eventHandlerList;
    private static final int DEFAULT_BUFFER_SIZE = 1024;
    private final String name;

    public DisruptorQueue(String name) {
        this.eventDisruptor = new Disruptor<>(NobuEvent::new, DEFAULT_BUFFER_SIZE, new ConsumerThreadFactory(name));
        this.eventHandlerList = new ArrayList<>();
        this.name = name;
        Log.info("Disruptor Initialized for the type:" + this.name);
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
