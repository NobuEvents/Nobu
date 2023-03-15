package com.nobu.config;

import com.nobu.exception.RouteConfigException;
import org.jboss.logging.Logger;

import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.util.Optional;
import java.util.function.Function;

public class SchemaDirReader {

    public static Path getSchemaDirectory(String path) {
        return new DirectSchemaDirectoryReader()
                .apply(path)
                .or(() -> new ResourceSchemaDirectoryReader().apply(path))
                .or(() -> new URLSchemaDirectoryReader().apply(path))
                .orElseThrow(() -> new RouteConfigException("Invalid config file path"));
    }

    public static class DirectSchemaDirectoryReader implements Function<String, Optional<Path>> {

        @Override
        public Optional<Path> apply(String path) {
            var file = new File(path);
            if (file.exists() && file.isDirectory()) {
                return Optional.of(Path.of(file.getAbsolutePath()));
            }
            return Optional.empty();
        }

    }

    public static class ResourceSchemaDirectoryReader implements Function<String, Optional<Path>> {

        @Override
        public Optional<Path> apply(String path) {
            var resource = getClass().getClassLoader().getResource(path);
            if (resource == null) {
                return Optional.empty();
            }
            var file = new File(resource.getFile());
            if (file.exists() && file.isDirectory()) {
                return Optional.of(Path.of(file.getAbsolutePath()));
            }
            return Optional.empty();
        }
    }

    public static class URLSchemaDirectoryReader implements Function<String, Optional<Path>> {

        public static final Logger LOG = Logger.getLogger(ConfigReader.URLConfigReader.class);

        @Override
        public Optional<Path> apply(String path) {
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
            var file = new File(uri);
            if (file.isDirectory()) {
                return Optional.of(Path.of(file.getAbsolutePath()));
            }
            return Optional.empty();
        }
    }
}
