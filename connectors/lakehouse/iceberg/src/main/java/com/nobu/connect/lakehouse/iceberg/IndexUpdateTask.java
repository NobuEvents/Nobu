package com.nobu.connect.lakehouse.iceberg;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Represents a unit of work for updating the PrimaryKeyIndex
 * following an external compaction (RewriteDataFiles).
 */
public class IndexUpdateTask {

    // Set of keys that are affected/moved
    // Ideally we pass the full mapping: Key -> NewRecordLocation
    private final Map<String, RecordLocation> newMappings;

    // For now we might not strictly need removedFiles if we just upsert new
    // mappings,
    // but useful for debugging or detailed cleanup if key was deleted (not just
    // moved).
    private final Set<String> removedFiles;

    public IndexUpdateTask(Map<String, RecordLocation> newMappings, Set<String> removedFiles) {
        this.newMappings = newMappings != null ? newMappings : Collections.emptyMap();
        this.removedFiles = removedFiles != null ? removedFiles : Collections.emptySet();
    }

    public Map<String, RecordLocation> getNewMappings() {
        return newMappings;
    }

    public Set<String> getRemovedFiles() {
        return removedFiles;
    }
}
