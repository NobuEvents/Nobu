package com.nobu.queue;

import io.micrometer.common.lang.Nullable;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Singleton;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


@Singleton
public class EventQueueFactory {

    private ConcurrentHashMap<String, EventQueue> queueMap;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();

    @PostConstruct
    public void initialize() {
        queueMap = new ConcurrentHashMap<>();
    }


    public void put(String type, EventQueue queue) {
        writeLock.lock();
        try {
            queueMap.put(type, queue);
        } finally {
            writeLock.unlock();
        }
    }

    @Nullable
    public EventQueue get(String type) {
        readLock.lock();
        try {
            return queueMap.get(type);
        } finally {
            readLock.unlock();
        }
    }

    public void start() {
        writeLock.lock();
        try {
            queueMap.values().forEach(EventQueue::start);
        } finally {
            writeLock.unlock();
        }
    }


    @PreDestroy
    public void terminate() {
        writeLock.lock();
        try {
            queueMap.clear();
        } finally {
            writeLock.unlock();
        }

    }
}
