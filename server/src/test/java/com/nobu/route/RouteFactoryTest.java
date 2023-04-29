package com.nobu.route;


import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RouteFactoryTest {

    private static RouteFactory routeFactory;

    @BeforeAll
    static void setup() {
        routeFactory = RouteFactory.get("route.yaml");
    }

    @Test
    public void testRouteFactoryConnection() {
        assertTrue(routeFactory.getConnectionMap().containsKey("front_kafka"));
    }

    @Test
    public void testRouteFactoryNotEmpty() {
        assertTrue(routeFactory.getRoutes().length > 0);
    }

    @Test
    public void testRouteFactoryValue() {
        assertEquals("front_kafka", routeFactory.getRoutes()[0].getTarget());
    }

}
