package com.nobu.connect.lakehouse.iceberg;

import com.nobu.spi.connect.Connector;
import com.nobu.spi.connect.Context;
import com.nobu.spi.event.NobuEvent;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IcebergCompactionIntegrationTest {

    private static final String TEST_DIR = "/tmp/iceberg_compaction_test";
    private static final String WAREHOUSE = TEST_DIR + "/warehouse";
    private IcebergConnector connector;

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

    @AfterEach
    public void tearDown() {
        if (connector != null)
            connector.shutdown();
    }

    @Test
    public void testCompactionHandling() throws Exception {
        // 1. Initialize Connector
        connector = new IcebergConnector();
        Context context = mock(Context.class);
        Map<String, String> config = new HashMap<>();
        config.put("warehouse.path", WAREHOUSE);
        config.put("batch.size", "1"); // Flush immediately
        when(context.connectConfig()).thenReturn(config);

        connector.initialize("test-target", context);

        // 2. Insert Data (Creates File A)
        connector.onEvent(createEvent("1", "OriginalData"), 1, true);

        // 3. Verify File A exists
        Configuration conf = new Configuration();
        HadoopCatalog catalog = new HadoopCatalog(conf, WAREHOUSE);
        TableIdentifier tableId = TableIdentifier.of("default", "users");
        Table table = catalog.loadTable(tableId);

        List<DataFile> filesA = StreamSupport
                .stream(table.currentSnapshot().addedDataFiles(table.io()).spliterator(), false)
                .collect(Collectors.toList());
        assertEquals(1, filesA.size());
        DataFile fileA = filesA.get(0);
        System.out.println("File A: " + fileA.path());

        // 4. Simulate Compaction (Rewrite A -> X)
        Path pathA = Paths.get(fileA.path().toString().replace("file:", ""));
        Path pathX = Paths.get(WAREHOUSE + "/data/file-x.parquet");
        Files.createDirectories(pathX.getParent());
        if (Files.exists(pathA)) {
            Files.copy(pathA, pathX);
        } else {
            // Fallback if path is URI
            Files.copy(Paths.get(java.net.URI.create(fileA.path().toString())), pathX);
        }

        DataFile fileX = org.apache.iceberg.DataFiles.builder(table.spec())
                .copy(fileA)
                .withPath(pathX.toUri().toString())
                .build();

        table.refresh();
        table.newRewrite()
                .rewriteFiles(Collections.singleton(fileA), Collections.singleton(fileX))
                .commit();

        System.out.println("Committed Rewrite: A -> X");

        // 5. Wait for Monitor (25s)
        System.out.println("Waiting for Monitor...");
        Thread.sleep(25000);

        // 6. Verify Connector processes mailbox
        connector.onEvent(createEvent("99", "Dummy"), 4, true);

        // 7. Verify Index has updated
        java.lang.reflect.Field indexField = IcebergConnector.class.getDeclaredField("primaryKeyIndex");
        indexField.setAccessible(true);
        PrimaryKeyIndex index = (PrimaryKeyIndex) indexField.get(connector);

        RecordLocation location = index.lookup("1");

        assertNotNull(location, "Index should contain key '1'");
        // Check method availability: likely getDataFile() or dataFile()
        String locationStr = location.toString();
        System.out.println("Index Location for '1': " + locationStr);

        assertTrue(locationStr.contains("file-x.parquet"), "Index should point to new file X");
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
