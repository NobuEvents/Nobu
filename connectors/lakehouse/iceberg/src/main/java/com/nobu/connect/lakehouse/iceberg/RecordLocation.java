package com.nobu.connect.lakehouse.iceberg;

import java.io.Serializable;
import java.util.Objects;

/**
 * Represents the physical location of a row in an Iceberg table.
 * Used by the PrimaryKeyIndex to track where the latest version of a row lives.
 */
public class RecordLocation implements Serializable {
    private final String dataFile;
    private final long rowId;

    public RecordLocation(String dataFile, long rowId) {
        this.dataFile = dataFile;
        this.rowId = rowId;
    }

    public String getDataFile() {
        return dataFile;
    }

    public long getRowId() {
        return rowId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        RecordLocation that = (RecordLocation) o;
        return rowId == that.rowId && Objects.equals(dataFile, that.dataFile);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataFile, rowId);
    }

    @Override
    public String toString() {
        return "RecordLocation{" +
                "dataFile='" + dataFile + '\'' +
                ", rowId=" + rowId +
                '}';
    }
}
