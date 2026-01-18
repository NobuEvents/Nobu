package com.nobu.connect.lakehouse.iceberg;

import org.apache.iceberg.*;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.jboss.logging.Logger;

import java.io.Closeable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Background thread that monitors the Iceberg table for "RewriteDataFiles"
 * (Compaction) operations.
 * When a compaction is detected, it calculates the index updates and pushes
 * them to the Mailbox.
 */
public class IcebergSnapshotMonitor implements Closeable {

    private static final Logger LOG = Logger.getLogger(IcebergSnapshotMonitor.class);
    private final Table table;
    private final Queue<IndexUpdateTask> mailbox;
    private final ScheduledExecutorService executor;
    private final AtomicBoolean running = new AtomicBoolean(true);

    private long lastSnapshotId = -1;

    public IcebergSnapshotMonitor(Table table, Queue<IndexUpdateTask> mailbox) {
        this.table = table;
        this.mailbox = mailbox;
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "iceberg-snapshot-monitor");
            t.setDaemon(true);
            return t;
        });

        // Initialize lastSnapshotId to current so we only process *new* compactions
        if (table.currentSnapshot() != null) {
            this.lastSnapshotId = table.currentSnapshot().snapshotId();
        }
    }

    public void start() {
        // Poll every 10 seconds
        executor.scheduleWithFixedDelay(this::checkForUpdates, 10, 10, TimeUnit.SECONDS);
        LOG.info("IcebergSnapshotMonitor started for table: " + table.name());
    }

    private void checkForUpdates() {
        if (!running.get())
            return;

        try {
            table.refresh();
            Snapshot current = table.currentSnapshot();

            if (current == null || current.snapshotId() == lastSnapshotId) {
                return;
            }

            LOG.info("Detected new snapshot: " + current.snapshotId() + " (Operation: " + current.operation() + ")");

            // Check if this is a compaction/rewrite
            if (isCompaction(current)) {
                processCompaction(current);
            }

            lastSnapshotId = current.snapshotId();

        } catch (Exception e) {
            LOG.error("Error in SnapshotMonitor polling loop", e);
        }
    }

    private boolean isCompaction(Snapshot snapshot) {
        String op = snapshot.operation();
        return DataOperations.REPLACE.equals(op);
    }

    private void processCompaction(Snapshot snapshot) {
        LOG.info("Processing compaction snapshot: " + snapshot.snapshotId());

        Set<String> removedFiles = new HashSet<>();
        Map<String, RecordLocation> newMappings = new HashMap<>();

        // 1. Identify Removed Files
        for (DataFile file : snapshot.removedDataFiles(table.io())) {
            removedFiles.add(file.path().toString());
        }

        if (removedFiles.isEmpty()) {
            LOG.info("Compaction snapshot has no removed files, skipping index update.");
            return;
        }

        // 2. Scan Added Files
        Iterable<DataFile> addedFiles = snapshot.addedDataFiles(table.io());
        for (DataFile file : addedFiles) {
            scanFileForKeys(file, newMappings);
        }

        // 3. Post to Mailbox
        if (!newMappings.isEmpty()) {
            mailbox.offer(new IndexUpdateTask(newMappings, removedFiles));
            LOG.info("Posted IndexUpdateTask to mailbox. Removed: " + removedFiles.size() + ", New Mappings: "
                    + newMappings.size());
        }
    }

    private void scanFileForKeys(DataFile file, Map<String, RecordLocation> newMappings) {
        // Use Parquet.read directly to scan the specific added file.
        // This bypasses IcebergGenerics table scan which is inefficient for this
        // purpose.
        try (CloseableIterable<org.apache.iceberg.data.Record> reader = Parquet
                .read(table.io().newInputFile(file.path().toString()))
                .project(table.schema())
                .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(table.schema(), fileSchema))
                .build()) {

            long rowId = 0;
            for (org.apache.iceberg.data.Record record : reader) {
                String key = extractKeyFromRecord(record);
                if (key != null) {
                    newMappings.put(key, new RecordLocation(file.path().toString(), rowId));
                }
                rowId++;
            }

        } catch (Throwable t) {
            LOG.error("Failed to scan compacted file: " + file.path(), t);
        }
    }

    private String extractKeyFromRecord(org.apache.iceberg.data.Record record) {
        if (record.getField("id") != null)
            return record.getField("id").toString();
        if (record.getField("key") != null)
            return record.getField("key").toString();
        if (record.getField("_id") != null)
            return record.getField("_id").toString();
        return null;
    }

    @Override
    public void close() {
        running.set(false);
        executor.shutdownNow();
    }
}
