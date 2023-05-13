package com.nobu.connect;

import com.lmax.disruptor.EventHandler;
import com.nobu.event.NobuEvent;


public interface Connector extends EventHandler<NobuEvent> {

  void initialize(String target, Context context);

  void shutdown();
}
