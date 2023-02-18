package com.nobu.route;


import java.util.Arrays;

public class RouteConfig {

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
}


