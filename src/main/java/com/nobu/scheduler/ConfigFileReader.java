package com.nobu.scheduler;

import java.io.File;
import java.util.Optional;
import java.util.function.Function;

/***
 * Read the configuration file and return a {@link File} object.
 */
public class ConfigFileReader implements Function<String, Optional<File>> {

    /**
     * Check if the file exists in a given absolute path, else fall back to reading from the classpath.
     *
     * @param path the function argument
     * @return Optional<File> of the configuration file
     */
    @Override
    public Optional<File> apply(String path) {
        var file = new File(path);
        if (!file.exists()) {
            return searchInRelativePath(path);
        }
        return Optional.of(file);
    }

    private Optional<File> searchInRelativePath(String path) {
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
