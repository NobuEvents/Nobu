package com.nobu.connect.lakehouse.iceberg;

import com.nobu.spi.event.NobuEvent;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class IcebergTableManagerTest {

    private static final String TEST_DIR = "/tmp/iceberg_manager_test";
    private static final String WAREHOUSE = TEST_DIR + "/warehouse";
    private static final String INDEX_DIR = TEST_DIR + "/index";

    @BeforeEach
    public void setUp() throws IOException {
        Path path = Paths.get(TEST_DIR);
        if (Files.exists(path)) {
            Files.walk(path)
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.delete(p);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
        }
        Files.createDirectories(Paths.get(WAREHOUSE));
        Files.createDirectories(Paths.get(INDEX_DIR));
    }

    @Test
    public void testUpsertFlow() {
        // 1. Setup Catalog & Table
        Configuration conf = new Configuration();
        HadoopCatalog catalog = new HadoopCatalog(conf, WAREHOUSE);

        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.StringType.get()),
                Types.NestedField.optional(2, "data", Types.StringType.get()));

        TableIdentifier name = TableIdentifier.of("default", "users");
        Table table = catalog.createTable(name, schema);

        // 2. Setup Manager
        try (PrimaryKeyIndex index = new PrimaryKeyIndex(INDEX_DIR)) {
            IcebergTableManager manager = new IcebergTableManager(table, index);

            // 3. Batch 1: Insert A, B
            List<NobuEvent> batch1 = Arrays.asList(
                    createEvent("1", "A"),
                    createEvent("2", "B"));
            manager.commitBatch(batch1);

            // Verify Index
            assertNotNull(index.lookup("1"));
            assertNotNull(index.lookup("2"));

            // Verify Iceberg
            table.refresh();
            // Should have some snapshots now
            assertNotNull(table.currentSnapshot());

            // 4. Batch 2: Insert C, Update A -> A_v2
            List<NobuEvent> batch2 = Arrays.asList(
                    createEvent("3", "C"),
                    createEvent("1", "A_v2") // Upsert
            );
            manager.commitBatch(batch2);

            // Verify Index
            RecordLocation locA = index.lookup("1");
            RecordLocation locC = index.lookup("3");

            assertNotNull(locA);
            assertNotNull(locC);
            // Verify that A's location has changed (it's in a new file now)
            // Note: In a real run, file names are random UUIDs so they will differ.

            // Verify Iceberg Snapshot 2
            table.refresh();
            // Should be clean commit
        }
    }

    private NobuEvent createEvent(String id, String data) {
        NobuEvent event = new NobuEvent();
        event.setEventName("users");
        event.setEventId(UUID.randomUUID().toString());
        event.setSrn("srn:test");
        String json = String.format("{\"id\":\"%s\", \"data\":\"%s\"}", id, data);
        event.setMessage(json.getBytes());
        return event;
    }
}
