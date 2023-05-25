package com.nobu.config;

import com.nobu.exception.RouteConfigException;
import com.nobu.exception.SchemaConfigException;
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
    }
}
