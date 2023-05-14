package com.nobu.connect;


import com.nobu.route.RouteFactory;

import com.nobu.spi.connect.Connector;
import com.nobu.spi.connect.Context;
import java.util.Optional;
import javax.inject.Singleton;

import java.util.Map;
import org.jboss.logging.Logger;


@Singleton
public class ConnectorFactory {

  private static final Logger LOG = Logger.getLogger(ConnectorFactory.class);

  public Optional<Connector> getConnector(String target,
      RouteFactory.Connection connection,
      Map<String, String> routeConfig) {

    try {
      Connector connector = (Connector) Class.forName(connection.getImpl()).getDeclaredConstructor().newInstance();
      Context context = new Context(connection.getType(), connection.getImpl(), connection.getConfig(), routeConfig);
      connector.initialize(target, context);
      return Optional.of(connector);
    } catch (Exception e) {
      LOG.error("Unable to initialize the connector:", e);
    }
    return Optional.empty();
  }
}
