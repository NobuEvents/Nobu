package com.nobu.connect.lakehouse.iceberg;

import com.nobu.spi.connect.Connector;
import com.nobu.spi.connect.Context;
import com.nobu.spi.event.NobuEvent;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IcebergConnectorIntegrationTest {

    private static final String TEST_DIR = "/tmp/iceberg_connector_test";
    private static final String WAREHOUSE = TEST_DIR + "/warehouse";

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
    }

    @Test
    public void testEndToEndFlow() throws Exception {
        // 1. Initialize Connector
        Connector connector = new IcebergConnector();
        Context context = mock(Context.class);
        Map<String, String> config = new HashMap<>();
        config.put("warehouse.path", WAREHOUSE);
        config.put("batch.size", "2"); // Small batch to force flush
        when(context.connectConfig()).thenReturn(config);

        connector.initialize("test-target", context);

        // 2. Setup Catalog for verification
        Configuration conf = new Configuration();
        HadoopCatalog catalog = new HadoopCatalog(conf, WAREHOUSE);
        TableIdentifier tableId = TableIdentifier.of("default", "users");

        // 3. Send Events
        // Event 1: Insert A
        connector.onEvent(createEvent("1", "A"), 1, false);
        // Event 2: Insert B (Trigger Flush due to batch size 2)
        connector.onEvent(createEvent("2", "B"), 2, false);

        // Verify Flush 1
        assertTrue(catalog.tableExists(tableId));
        Table table = catalog.loadTable(tableId);
        assertEquals(1, table.currentSnapshot().allManifests(table.io()).size());

        // Event 3: Upsert A -> A_v2
        connector.onEvent(createEvent("1", "A_v2"), 3, false);
        // Event 4: Insert C (Trigger Flush)
        connector.onEvent(createEvent("3", "C"), 4, true);

        // Verify Flush 2
        table.refresh();
        // Should have handled upsert via Index and Position Delete
        // Simple assertion: Snapshot sequence advanced
        assertEquals(2, table.history().size());

        // 4. Verify WAL Truncation (Indirectly)
        // If WAL wasn't truncating, we'd eventually run out of disk space or seqIds
        // would be messy.
        // For now, just ensuring no exceptions is good.

        connector.shutdown();
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
