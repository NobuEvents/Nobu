package com.nobu.connect.lakehouse.iceberg;

import com.nobu.spi.connect.Connector;
import com.nobu.spi.connect.Context;
import com.nobu.spi.event.NobuEvent;

public class IcebergConnector implements Connector {
  @Override
  public void initialize(String target, Context context) {
    System.out.println("IcebergConnector.initialize");
  }

  @Override
  public void shutdown() {
    System.out.println("IcebergConnector.shutdown");
  }

  @Override
  public void onEvent(NobuEvent nobuEvent, long l, boolean b) throws Exception {
    System.out.println("IcebergConnector.onEvent" + nobuEvent.toString());
  }
}
