package com.nobu.config;

import com.nobu.exception.RouteConfigException;
import org.jboss.logging.Logger;

import java.io.File;
import java.net.URI;
import java.util.Optional;
import java.util.function.Function;

public class ConfigReader {

    public static File getRouteConfig(String path) {
        return new FileConfigReader()
                .apply(path)
                .or(() -> new ResourceConfigReader().apply(path))
                .or(() -> new URLConfigReader().apply(path))
                .orElseThrow(() -> new RouteConfigException("Invalid config file path" + path));
    }

    public static class FileConfigReader implements Function<String, Optional<File>> {

        @Override
        public Optional<File> apply(String path) {
            var file = new File(path);
            if (file.exists()) {
                return Optional.of(file);
            }
            return Optional.empty();
        }

    }

    public static class ResourceConfigReader implements Function<String, Optional<File>> {

        @Override
        public Optional<File> apply(String path) {
            var resource = getClass().getClassLoader().getResource(path);
            if (resource == null) {
                return Optional.empty();
            }
            var file = new File(resource.getFile());
            if (file.exists()) {
                return Optional.of(file);
            }
            return Optional.empty();
        }
    }

    public static class URLConfigReader implements Function<String, Optional<File>> {

        public static final Logger LOG = Logger.getLogger(URLConfigReader.class);

        @Override
        public Optional<File> apply(String path) {
            URI uri;
            try {
                uri = new URI(path);
                if (uri.getScheme() == null) {
                    return Optional.empty();
                }
            } catch (Exception e) {
                LOG.error("Invalid URI: " + path);
                return Optional.empty();
            }
            return Optional.of(new File(uri));
        }
    }
}
