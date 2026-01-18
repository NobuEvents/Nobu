package com.nobu.queue;

import org.jboss.logging.Logger;

public class ConsumerUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

    private static final Logger LOG = Logger.getLogger(ConsumerUncaughtExceptionHandler.class);

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        if (t.isAlive()) {
            LOG.error(String.format("%s is alive but throwing exception", t.getName()), e);
        } else if (t.isInterrupted()) {
            LOG.error(String.format("%s is interrupted and throwing exception", t.getName()), e);
        } else {
            LOG.error(String.format("%s throwing exception", t.getName()), e);
        }

    }
}
