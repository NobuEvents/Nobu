package com.nobu.route;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.nobu.config.ConfigReader;
import com.nobu.config.YamlReader;
import com.nobu.exception.RouteConfigException;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.util.Arrays;

public class RouteFactory {

    private static final Logger LOG = Logger.getLogger(RouteFactory.class);

    public Route[] routes;

    public Route[] getRoutes() {
        return routes;
    }

    public void setRoutes(Route[] routes) {
        this.routes = routes;
    }

    @Override
    public String toString() {
        return "Config{" + "routes=" + Arrays.toString(routes) + '}';
    }

    @JsonIgnore
    public static RouteFactory get(String path) throws RouteConfigException {
        try {
            return YamlReader.getYaml(ConfigReader.getRouteConfig(path), RouteFactory.class);
        } catch (IOException e) {
            LOG.error("Route config not able to read", e);
            throw new RouteConfigException("Exception while reading route config", e);
        }
    }

}


