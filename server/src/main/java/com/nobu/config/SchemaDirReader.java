package com.nobu.config;


import com.nobu.exception.SchemaConfigException;
import org.jboss.logging.Logger;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.function.Function;

public class SchemaDirReader {

    public static Path getSchemaDirectory(String path) {
        // Normalize path - remove leading ./ if present
        String normalizedPath = path.startsWith("./") ? path.substring(2) : path;
        
        return new DirectSchemaDirectoryReader()
                .apply(normalizedPath)
                .or(() -> new ResourceSchemaDirectoryReader().apply(normalizedPath))
                .or(() -> new URLSchemaDirectoryReader().apply(normalizedPath))
                .orElseThrow(() -> new SchemaConfigException("Invalid config file path: " + path + " (normalized: " + normalizedPath + ")"));
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
            
            try {
                // Handle both file system resources and JAR resources
                URI uri = resource.toURI();
                if (uri.getScheme().equals("jar")) {
                    // For JAR resources, we can't use File API
                    // Return empty to let other readers try, or handle specially
                    return Optional.empty();
                } else {
                    // For file system resources (like in tests)
                    Path resourcePath = Paths.get(uri);
                    if (Files.exists(resourcePath) && Files.isDirectory(resourcePath)) {
                        return Optional.of(resourcePath);
                    }
                }
            } catch (Exception e) {
                // If URI conversion fails, try File API as fallback
                try {
                    var file = new File(resource.getFile());
                    if (file.exists() && file.isDirectory()) {
                        return Optional.of(Path.of(file.getAbsolutePath()));
                    }
                } catch (Exception ex) {
                    // Ignore and return empty
                }
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
