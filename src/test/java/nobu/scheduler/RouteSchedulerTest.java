package nobu.scheduler;

import com.nobu.scheduler.RouteScheduler;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;


import javax.inject.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
public class RouteSchedulerTest {

    @Inject
    RouteScheduler scheduler;
    @Test
    public void readRouteConfigTest() {
        assertEquals(2, scheduler.getRouteConfig().getRoutes().length);
    }
}
