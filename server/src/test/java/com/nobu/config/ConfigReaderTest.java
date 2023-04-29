package com.nobu.config;

import com.nobu.exception.RouteConfigException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ConfigReaderTest {

    @Test
    public void testRouteConfigFile() {
        assert ConfigReader.getRouteConfig("route.yaml").exists();
    }

    @Test
    public void testRouteConfigFileNotFound() {

        var exception = assertThrows(RouteConfigException.class, () -> {
            ConfigReader.getRouteConfig("route1.yaml");
        });
        assertEquals("Invalid config file path", exception.getMessage());
    }

    @Test
    public void testSubDirectoryConfigFile() {
        assert ConfigReader.getRouteConfig("model/activity.schema.yaml").exists();
    }

    @Test
    public void testSubDirectoryConfigFileNotFound() {
        var exception = assertThrows(RouteConfigException.class, () -> {
            ConfigReader.getRouteConfig("model/signup1.schema.yaml");
        });
        assertEquals("Invalid config file path", exception.getMessage());
    }
}
