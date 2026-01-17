package com.nobu.connect.lakehouse.iceberg;

import com.nobu.spi.event.NobuEvent;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class IcebergFoundationTest {

    private static final String TEST_DIR = "/tmp/iceberg_foundation_test";

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
    }

    @Test
    public void testBufferManager() {
        try (IcebergBufferManager bufferManager = new IcebergBufferManager(TEST_DIR)) {
            // 1. Append events
            NobuEvent event1 = createEvent("evt1", "key1");
            NobuEvent event2 = createEvent("evt2", "key2");

            long id1 = bufferManager.append(event1);
            long id2 = bufferManager.append(event2);

            assertTrue(id1 > 0);
            assertTrue(id2 > id1);

            // 2. Recover
            List<NobuEvent> recovered = bufferManager.recover();
            assertEquals(2, recovered.size());
            assertEquals("evt1", recovered.get(0).getEventName());
            assertEquals("evt2", recovered.get(1).getEventName());

            // 3. Truncate
            bufferManager.truncate(id1);
            List<NobuEvent> recoveredAfterTruncate = bufferManager.recover();
            assertEquals(1, recoveredAfterTruncate.size());
            assertEquals("evt2", recoveredAfterTruncate.get(0).getEventName());
        }
    }

    @Test
    public void testPrimaryKeyIndex() {
        try (PrimaryKeyIndex index = new PrimaryKeyIndex(TEST_DIR)) {
            // 1. Initial Insert
            String key = "user-123";
            RecordLocation loc1 = new RecordLocation("file-A.parquet", 100);
            index.upsert(key, loc1);

            RecordLocation lookup1 = index.lookup(key);
            assertEquals(loc1, lookup1);

            // 2. Upsert (Move to new file)
            RecordLocation loc2 = new RecordLocation("file-B.parquet", 50);
            index.upsert(key, loc2);

            RecordLocation lookup2 = index.lookup(key);
            assertEquals(loc2, lookup2);

            // 3. Check Deletion Vector for file-A
            RoaringBitmap dvA = index.getDeletionVector("file-A.parquet");
            assertTrue(dvA.contains(100)); // Old row should be marked deleted

            // 4. Check Deletion Vector for file-B
            RoaringBitmap dvB = index.getDeletionVector("file-B.parquet");
            assertTrue(dvB.isEmpty()); // New row is active
        }
    }

    private NobuEvent createEvent(String name, String key) {
        NobuEvent event = new NobuEvent();
        event.setEventName(name);
        event.setEventId(UUID.randomUUID().toString());
        event.setMessage(("{\"id\":\"" + key + "\"}").getBytes());
        return event;
    }
}
