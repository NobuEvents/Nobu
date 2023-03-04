package com.nobu.scheduler;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;


import javax.inject.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
public class RouteSchedulerTest {

    @Inject
    RouteScheduler scheduler;

    @Test
    public void readRouteConfigTest() {
        assertTrue(scheduler.getRouteConfig().getRoutes().length > 1);
    }
}
