package com.nobu;

import java.nio.file.Path;
import java.nio.file.Paths;

public class ResourceLoader {

    public static String getRoutes() {
        Path resourceDirectory = Paths.get("src", "test", "resources");
        String absolutePath = resourceDirectory.toFile().getAbsolutePath();
        return absolutePath + "/route.yaml";
    }

    public static String getSchemas() {
        Path resourceDirectory = Paths.get("src", "test", "resources");
        String absolutePath = resourceDirectory.toFile().getAbsolutePath();
        return absolutePath + "/model/activity.schema.yaml";
    }

}
