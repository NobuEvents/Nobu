package com.nobu.connect;


import com.nobu.exception.RouteConfigException;
import com.nobu.route.RouteFactory;


import javax.inject.Singleton;

import java.util.Map;


@Singleton
public class ConnectorFactory {


    public Connector getConnector(String target,
                                  RouteFactory.Connection connection,
                                  Map<String, String> routeConfig) {

        try {
            Connector connector = (Connector) Class.forName(connection.getImpl()).getDeclaredConstructor().newInstance();
            connector.initialize(target, connection, routeConfig);
            return connector;
        } catch (Exception e) {
            throw new RouteConfigException("Unable to initialize the connector:", e);
        }
    }

}
