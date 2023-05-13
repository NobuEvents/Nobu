package com.nobu.spi.connect.console;

import com.nobu.spi.connect.Connector;
import com.nobu.spi.connect.Context;
import com.nobu.event.NobuEvent;


public class ConsoleConnector implements Connector {
  @Override
  public void initialize(String target, Context context){
    System.out.println("ConsoleConnector.initialize");
  }

  @Override
  public void shutdown() {
    System.out.println("ConsoleConnector.shutdown");
  }

  @Override
  public void onEvent(NobuEvent nobuEvent, long l, boolean b)
      throws Exception {
    System.out.println("ConsoleConnector.onEvent" + nobuEvent.toString());
  }
}
