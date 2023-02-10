package com.nobu.queue;

import java.util.concurrent.ThreadFactory;

public class ConsumerThreadFactory implements ThreadFactory {

    private final String name;

    public ConsumerThreadFactory(String name) {
        this.name = name;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setDaemon(false);
        t.setName(name);
        t.setUncaughtExceptionHandler(new ConsumerUncaughtExceptionHandler());
        return t;
    }
}
