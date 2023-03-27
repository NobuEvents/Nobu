package com.nobu.route;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.nobu.config.ConfigReader;
import com.nobu.config.YamlReader;
import com.nobu.exception.RouteConfigException;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class RouteFactory {

    private static final Logger LOG = Logger.getLogger(RouteFactory.class);

    public Route[] routes;

    public Route[] getRoutes() {
        return routes;
    }

    public void setRoutes(Route[] routes) {
        this.routes = routes;
    }

    @JsonProperty("connections")
    private Map<String, Connection> connectionMap;

    public Map<String, Connection> getConnectionMap() {
        return connectionMap;
    }

    public void setConnectionMap(Map<String, Connection> connectionMap) {
        this.connectionMap = connectionMap;
    }

    public static class Connection {
        private String type;
        private String impl;
        private Map<String, String> config;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public Map<String, String> getConfig() {
            return config;
        }

        public String getImpl() {
            return impl;
        }

        public void setImpl(String impl) {
            this.impl = impl;
        }

        public void setConfig(Map<String, String> config) {
            this.config = config;
        }
    }

    public static class Route {

        private String type;
        private String target;
        private Map<String, String> config;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getTarget() {
            return target;
        }

        public void setTarget(String target) {
            this.target = target;
        }

        public Map<String, String> getConfig() {
            return config;
        }

        public void setConfig(Map<String, String> config) {
            this.config = config;
        }

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

    @JsonIgnore
    public void validateRouteConfig() throws RouteConfigException {
        for (var route : this.getRoutes()) {
            if (!this.getConnectionMap().containsKey(route.getTarget())) {
                throw new RouteConfigException("Invalid Route Configuration: "
                        + route.getTarget()
                        + " not found in the connection");
            }
        }

    }

    @Override
    public String toString() {
        return "Config{" + "routes=" + Arrays.toString(routes) + '}';
    }

}


