package com.nobu.schema;

import com.nobu.cel.CelManager;
import com.nobu.config.SchemaDirReader;

import io.quarkus.runtime.Startup;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/***
 * Load the schema files from the schema directory
 * The schema files are loaded into a map
 * The map key is the schema name and the value is the schema object
 */
@Startup
@ApplicationScoped
public class SchemaLoader {

    private static final Logger LOG = Logger.getLogger(SchemaLoader.class);


    @Inject
    CelManager celManager;

    private final Map<String, SchemaFactory.Schema> schemaMap = new ConcurrentHashMap<>();

    /***
     * Read the schema files recursively
     * config: path -  The path to the directory where the schema files are located
     * config: ext - The file extension of the schema files
     */
    @PostConstruct
    public void initialize() {
        LOG.info("Initializing Schema Loader");
        var schemaDir = ConfigProvider.getConfig().getValue("schema.dir", String.class);
        var schemaExt = ConfigProvider.getConfig().getValue("schema.ext", String.class);

        readFilesRecursively(schemaDir, schemaExt);

        buildCelScript(schemaDir);
    }

    /***
     * Build the CEL script for each schema
     * @param schemaDir The directory where the schema files are located
     */
    private void buildCelScript(String schemaDir) {
        if (schemaMap.isEmpty()) {
            LOG.error("No schema files found in " + schemaDir);
            return;
        }

        schemaMap.forEach((k, v) -> {
            if (v.getTest() != null && v.getTest().get("dlq") != null && v.getTest().get("dlq").getQuery() != null) {
                LOG.info("Registering CEL script for " + k);
                celManager.addScript(k, new String[]{v.getTest().get("dlq").getQuery()});
            }
        });
    }

    public CelManager getCelManager() {
        return celManager;
    }

    /***
     * Get the schema map
     * @return Map of schema names and schema objects
     */
    public Map<String, SchemaFactory.Schema> getSchemaMap() {
        return schemaMap;
    }

    /**
     * Recursively read all files in a directory with a given file extension
     * Exception handle silently to log the error to make sure route is working without schema validation
     *
     * @param directoryPath       The path to the directory to read
     * @param fileExtensionFilter The file extension to filter by
     */
    private void readFilesRecursively(String directoryPath, String fileExtensionFilter) {

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(SchemaDirReader.getSchemaDirectory(directoryPath))) {
            for (Path path : stream) {
                if (Files.isDirectory(path)) {
                    // Recursively call the method if the path is a directory
                    readFilesRecursively(path.toFile().getAbsolutePath(), fileExtensionFilter);
                } else if (path.toString().endsWith(fileExtensionFilter)) {
                    SchemaFactory.get(path.toString()).ifPresent(schema -> schemaMap.putAll(schema.getSchemaMap()));
                }
            }
        } catch (IOException e) {
            LOG.error("Error reading schema files" + directoryPath, e);
        }
    }

}
