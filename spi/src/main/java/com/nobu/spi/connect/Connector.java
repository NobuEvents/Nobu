package com.nobu.spi.connect;

import com.lmax.disruptor.EventHandler;
import com.nobu.spi.event.NobuEvent;


public interface Connector extends EventHandler<NobuEvent> {

  void initialize(String target, Context context);

  void shutdown();
}
