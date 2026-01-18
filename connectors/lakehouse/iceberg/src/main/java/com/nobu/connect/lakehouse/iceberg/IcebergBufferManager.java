package com.nobu.connect.lakehouse.iceberg;

import com.nobu.spi.event.NobuEvent;
import org.jboss.logging.Logger;
import org.lmdbjava.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.Env.create;

/**
 * Manages buffering of events for Iceberg using a Write-Ahead Log (WAL) backed
 * by LMDB.
 * This ensures zero-data-loss durability before events are flushed to
 * Parquet/Iceberg.
 * <p>
 * Architecture:
 * 1. Append Path: Event -> LMDB (WAL) -> In-Memory Buffer
 * 2. Flush Path: In-Memory Buffer -> Iceberg Parquet Commit -> Truncate WAL
 * 3. Recovery Path: Startup -> Replay LMDB -> In-Memory Buffer
 */
public class IcebergBufferManager implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(IcebergBufferManager.class);
    private static final String DB_NAME = "iceberg_wal";
    private static final long MAP_SIZE = 10 * 1024 * 1024 * 1024L; // 10 GB

    private final Env<ByteBuffer> env;
    private final Dbi<ByteBuffer> db;
    private final AtomicLong sequenceIdGenerator = new AtomicLong(0);
    private final Path dbPath;

    public IcebergBufferManager(String warehousePath) {
        try {
            this.dbPath = Paths.get(warehousePath, "_wal_lmdb");
            if (!Files.exists(dbPath)) {
                Files.createDirectories(dbPath);
            }

            this.env = create()
                    .setMapSize(MAP_SIZE)
                    .setMaxDbs(1)
                    .open(dbPath.toFile());

            this.db = env.openDbi(DB_NAME, MDB_CREATE);

            // Initialize sequence ID from last known key
            initializeSequenceId();

            LOG.info("IcebergBufferManager initialized at: " + dbPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize IcebergBufferManager", e);
        }
    }

    private void initializeSequenceId() {
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            try (CursorIterable<ByteBuffer> cursor = db.iterate(txn, KeyRange.all())) {
                for (CursorIterable.KeyVal<ByteBuffer> kv : cursor) {
                    long id = kv.key().getLong(0);
                    if (id > sequenceIdGenerator.get()) {
                        sequenceIdGenerator.set(id);
                    }
                }
            }
        }
    }

    /**
     * Appends an event to the persistent WAL.
     * 
     * @param event The event to append
     * @return The assigned WAL sequence ID
     */
    public long append(NobuEvent event) {
        long seqId = sequenceIdGenerator.incrementAndGet();

        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            ByteBuffer key = ByteBuffer.allocateDirect(8);
            key.putLong(seqId).flip();

            byte[] serialized = serialize(event);
            ByteBuffer val = ByteBuffer.allocateDirect(serialized.length);
            val.put(serialized).flip();

            db.put(txn, key, val);
            txn.commit();
        } catch (Exception e) {
            throw new RuntimeException("Failed to append event to WAL", e);
        }

        return seqId;
    }

    /**
     * Recovers all pending events from the WAL.
     * Should be called on startup.
     */
    public List<NobuEvent> recover() {
        List<NobuEvent> events = new ArrayList<>();
        LOG.info("Recovering events from WAL...");

        try (Txn<ByteBuffer> txn = env.txnRead()) {
            try (CursorIterable<ByteBuffer> cursor = db.iterate(txn, KeyRange.all())) {
                for (CursorIterable.KeyVal<ByteBuffer> kv : cursor) {
                    byte[] bytes = new byte[kv.val().remaining()];
                    kv.val().get(bytes);
                    events.add(deserialize(bytes));
                }
            }
        }

        LOG.info("Recovered " + events.size() + " events from WAL.");
        return events;
    }

    /**
     * Truncates the WAL up to the given sequence ID.
     * Should be called after a successful Iceberg commit.
     */
    public void truncate(long upToSequenceId) {
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            // Reverting to safe iteration + delete (O(N log N)) to ensure correctness.
            // Direct Cursor.delete() proved unstable in tests (deleting too many records).
            try (CursorIterable<ByteBuffer> cursor = db.iterate(txn, KeyRange.all())) {
                for (CursorIterable.KeyVal<ByteBuffer> kv : cursor) {
                    long id = kv.key().getLong(0);
                    if (id <= upToSequenceId) {
                        db.delete(txn, kv.key());
                    } else {
                        // Keys are sorted, so once we exceed, we stop
                        break;
                    }
                }
            }
            txn.commit();
        }
    }

    @Override
    public void close() {
        env.close();
    }

    // --- Serialization Helpers ---

    private byte[] serialize(NobuEvent event) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(event);
            return bos.toByteArray();
        }
    }

    private NobuEvent deserialize(byte[] bytes) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis)) {
            return (NobuEvent) ois.readObject();
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize NobuEvent from WAL", e);
        }
    }
}
