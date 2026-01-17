package com.nobu.queue;

import io.micrometer.common.lang.Nullable;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Singleton;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
public class EventQueueFactory {

    private ConcurrentHashMap<String, EventQueue> queueMap;

    @PostConstruct
    public void initialize() {
        queueMap = new ConcurrentHashMap<>();
    }

    public void put(String type, EventQueue queue) {
        queueMap.put(type, queue);
    }

    @Nullable
    public EventQueue get(String type) {
        return queueMap.get(type);
    }

    public void start() {
        queueMap.values().forEach(EventQueue::start);
    }

    @PreDestroy
    public void terminate() {
        queueMap.clear();
    }
}
