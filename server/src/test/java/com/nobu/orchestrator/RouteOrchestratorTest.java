package com.nobu.orchestrator;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;


import javax.inject.Inject;

import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
public class RouteOrchestratorTest {

    @Inject
    RouteOrchestrator scheduler;

    @Test
    public void readRouteConfigTest() {
        assertTrue(scheduler.getRouteConfig().getRoutes().length > 1);
    }
}
