package com.nobu.connect.lakehouse.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nobu.spi.event.NobuEvent;
import org.apache.iceberg.*;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Core engine for High-Throughput Iceberg ingestion.
 * Orchestrates the "Flush" operation:
 * 1. Checks PrimaryKeyIndex for Upserts.
 * 2. Generating DataFiles (New Rows).
 * 3. Generating DeleteFiles (Position Deletes for Old Rows).
 * 4. Commits using RowDelta.
 */
public class IcebergTableManager {

    private static final Logger LOG = Logger.getLogger(IcebergTableManager.class);
    private final Table table;
    private final PrimaryKeyIndex index;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public IcebergTableManager(Table table, PrimaryKeyIndex index) {
        this.table = table;
        this.index = index;
    }

    public void commitBatch(List<NobuEvent> events) {
        if (events.isEmpty())
            return;

        LOG.info("Committing batch of size: " + events.size() + " to table: " + table.name());

        FileIO io = table.io();
        Schema schema = table.schema();
        PartitionSpec spec = table.spec();

        try {
            // 1. Prepare Writers

            // Data Writer
            String dataFileName = "data-" + UUID.randomUUID() + ".parquet";
            OutputFile dataOut = io.newOutputFile(table.locationProvider().newDataLocation(dataFileName));
            FileAppender<Record> dataAppender = Parquet.write(dataOut)
                    .schema(schema)
                    .createWriterFunc(GenericParquetWriter::buildWriter)
                    .build();

            // Delete Writer (Position Deletes)
            String deleteFileName = "delete-" + UUID.randomUUID() + ".parquet";
            OutputFile deleteOut = io.newOutputFile(table.locationProvider().newDataLocation(deleteFileName));

            // Note: We remove .rowSchema(schema) because we are writing Void position
            // deletes
            // and do not want to persist row data. providing rowSchema causes NPE in
            // GenericParquetWriter
            // if we don't provide actual rows.
            PositionDeleteWriter<Void> deleteWriter = Parquet.writeDeletes(deleteOut)
                    .createWriterFunc(GenericParquetWriter::buildWriter)
                    .withPartition(null) // Unpartitioned for MVP
                    .overwrite()
                    .withSpec(spec)
                    .buildPositionWriter();

            List<RecordLocation> newLocations = new ArrayList<>();
            List<String> keysToUpdate = new ArrayList<>();

            try (FileAppender<Record> appender = dataAppender;
                    PositionDeleteWriter<Void> dWriter = deleteWriter) {

                long currentRowId = 0;

                for (NobuEvent event : events) {
                    String key = extractKey(event);
                    Record record = convertToRecord(event, schema);

                    // Check Index for Upsert
                    RecordLocation oldLoc = index.lookup(key);
                    if (oldLoc != null) {
                        // Mark old row as deleted
                        PositionDelete<Void> delete = PositionDelete.create();
                        delete.set(oldLoc.getDataFile(), oldLoc.getRowId(), null);
                        dWriter.write(delete);
                    }

                    // Write new record
                    appender.add(record);

                    // Defer index update until commit succeeds
                    newLocations.add(new RecordLocation(dataFileName, currentRowId));
                    keysToUpdate.add(key);
                    currentRowId++;
                }
            }

            // 2. Commit (with Retry)
            // Re-construct metrics as DataFile
            DataFile finalDataFile = DataFiles.builder(spec)
                    .withPath(dataOut.location())
                    .withFileSizeInBytes(dataAppender.length())
                    .withRecordCount(dataAppender.metrics().recordCount())
                    .withFormat(FileFormat.PARQUET)
                    .build();

            DeleteWriteResult result = deleteWriter.result();
            final List<org.apache.iceberg.DeleteFile> deleteFiles = (result != null && result.deleteFiles() != null)
                    ? result.deleteFiles()
                    : Collections.emptyList();

            int attempts = 0;
            while (attempts < 3) {
                try {
                    commitToIceberg(finalDataFile, deleteFiles);
                    break;
                } catch (org.apache.iceberg.exceptions.CommitFailedException e) {
                    attempts++;
                    if (attempts >= 3) {
                        LOG.error("Failed to commit batch after 3 attempts", e);
                        throw e;
                    }
                    LOG.warn("Commit failed, retrying attempt " + attempts, e);
                    try {
                        Thread.sleep(100L * (1 << attempts)); // Exponential backoff: 200, 400, 800ms
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted during commit retry", ie);
                    }
                }
            }

            LOG.info("Committed batch. DataFile: " + dataFileName + ", P-Deletes: " + deleteFiles.size());

            // 3. Update Index (Post-Commit)
            for (int i = 0; i < keysToUpdate.size(); i++) {
                index.upsert(keysToUpdate.get(i), newLocations.get(i));
            }

        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write/commit files", e);
        }
    }

    private void commitToIceberg(DataFile dataFile, List<org.apache.iceberg.DeleteFile> deleteFiles) {
        table.refresh(); // Refresh before new attempt
        RowDelta rowDelta = table.newRowDelta();
        rowDelta.addRows(dataFile);

        deleteFiles.stream()
                .filter(df -> df.recordCount() > 0)
                .forEach(rowDelta::addDeletes);

        rowDelta.commit();
    }

    private String extractKey(NobuEvent event) {
        try {
            JsonNode node = objectMapper.readTree(event.getMessage());
            if (node.has("id"))
                return node.get("id").asText();
            if (node.has("key"))
                return node.get("key").asText();
            if (node.has("_id"))
                return node.get("_id").asText();
            return event.getSrn();
        } catch (Exception e) {
            return event.getSrn();
        }
    }

    private Record convertToRecord(NobuEvent event, Schema schema) {
        GenericRecord record = GenericRecord.create(schema);
        try {
            JsonNode json = objectMapper.readTree(event.getMessage());
            schema.columns().forEach(field -> {
                String name = field.name();
                if (json.has(name)) {
                    JsonNode val = json.get(name);
                    Object iceVal = convertJsonValue(val, field.type());
                    record.setField(name, iceVal);
                }
            });
            // Metadata
            if (schema.findField("_event_id") != null)
                record.setField("_event_id", event.getEventId());
            if (schema.findField("_timestamp") != null)
                record.setField("_timestamp", event.getTimestamp());
        } catch (Exception e) {
            LOG.warn("Failed to parse event JSON", e);
        }
        return record;
    }

    private Object convertJsonValue(JsonNode value, Type type) {
        if (value == null || value.isNull())
            return null;
        switch (type.typeId()) {
            case STRING:
                return value.asText();
            case LONG:
                return value.asLong();
            case INTEGER:
                return value.asInt();
            case BOOLEAN:
                return value.asBoolean();
            case DOUBLE:
                return value.asDouble();
            case FLOAT:
                return (float) value.asDouble();
            case TIMESTAMP:
                return LocalDateTime.now(); // Placeholder
            default:
                return value.asText();
        }
    }
}
