package com.nobu.connect.lakehouse.iceberg;

import org.jboss.logging.Logger;
import org.lmdbjava.*;
import org.roaringbitmap.RoaringBitmap;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.Env.create;

/**
 * Managing Primary Key mappings and Deletion Vectors for Real-Time ingestion.
 * real-time updates.
 * <p>
 * Components:
 * 1. KV Store (LMDB): Maps PrimaryKey (String) -> RecordLocation (File, Row).
 * 2. Deletion Vectors (Memory/Roaring): Maps File -> Bitmap of deleted row IDs.
 */
public class PrimaryKeyIndex implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(PrimaryKeyIndex.class);
    private static final String DB_NAME = "primary_key_index";
    private static final long MAP_SIZE = 10 * 1024 * 1024 * 1024L; // 10 GB

    private final Env<ByteBuffer> env;
    private final Dbi<ByteBuffer> db;

    // In-memory cache of Deletion Vectors (File -> RowIDs)
    // In a full implementation, these should be spilled to Puffin files and cleared
    // from memory.
    private final Map<String, RoaringBitmap> deletionVectors = new ConcurrentHashMap<>();

    public PrimaryKeyIndex(String warehousePath) {
        try {
            Path dbPath = Paths.get(warehousePath, "_index_lmdb");
            if (!Files.exists(dbPath)) {
                Files.createDirectories(dbPath);
            }

            this.env = create()
                    .setMapSize(MAP_SIZE)
                    .setMaxDbs(1)
                    .open(dbPath.toFile());

            this.db = env.openDbi(DB_NAME, MDB_CREATE);
            LOG.info("PrimaryKeyIndex initialized at: " + dbPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize PrimaryKeyIndex", e);
        }
    }

    /**
     * Looks up the current location of a primary key.
     */
    public RecordLocation lookup(String key) {
        if (key == null)
            return null;

        try (Txn<ByteBuffer> txn = env.txnRead()) {
            ByteBuffer keyBytes = stringToBuffer(key);
            ByteBuffer valBytes = db.get(txn, keyBytes);

            if (valBytes == null) {
                return null;
            }

            byte[] bytes = new byte[valBytes.remaining()];
            valBytes.get(bytes);
            return deserializeLocation(bytes);
        }
    }

    /**
     * Updates the location of a key.
     * If the key existed, marks the OLD location as deleted in the Deletion Vector.
     */
    public void upsert(String key, RecordLocation newLocation) {
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            ByteBuffer keyBytes = stringToBuffer(key);
            ByteBuffer existingVal = db.get(txn, keyBytes);

            if (existingVal != null) {
                // Key exists: Mark old row as deleted
                byte[] bytes = new byte[existingVal.remaining()];
                existingVal.get(bytes);
                RecordLocation oldLoc = deserializeLocation(bytes);

                markDeleted(oldLoc);
            }

            // Write new location
            ByteBuffer newVal = ByteBuffer.allocateDirect(1024); // simplistic sizing
            byte[] serialized = serializeLocation(newLocation);
            newVal.put(serialized).flip();

            db.put(txn, keyBytes, newVal);
            txn.commit();
        } catch (IOException e) {
            throw new RuntimeException("Failed to upsert key: " + key, e);
        }
    }

    /**
     * Marks a specific location as deleted.
     */
    private void markDeleted(RecordLocation loc) {
        deletionVectors.compute(loc.getDataFile(), (k, v) -> {
            if (v == null) {
                v = new RoaringBitmap();
            }
            v.add((int) loc.getRowId()); // RoaringBitmap uses int, RowID should fit
            return v;
        });
    }

    /**
     * Returns the current deletion vector for a file (as a copy).
     * Used by the Committer to write Puffin files.
     */
    public RoaringBitmap getDeletionVector(String dataFile) {
        RoaringBitmap map = deletionVectors.get(dataFile);
        return map != null ? map.clone() : new RoaringBitmap();
    }

    /**
     * Clears the in-memory deletion vector for a file (after it has been flushed to
     * Puffin).
     */
    public void clearDeletionVector(String dataFile) {
        deletionVectors.remove(dataFile);
    }

    @Override
    public void close() {
        env.close();
    }

    // --- Helpers ---

    private ByteBuffer stringToBuffer(String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        ByteBuffer bb = ByteBuffer.allocateDirect(b.length);
        bb.put(b).flip();
        return bb;
    }

    private byte[] serializeLocation(RecordLocation loc) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(loc);
            return bos.toByteArray();
        }
    }

    private RecordLocation deserializeLocation(byte[] bytes) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis)) {
            return (RecordLocation) ois.readObject();
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize RecordLocation", e);
        }
    }
}
