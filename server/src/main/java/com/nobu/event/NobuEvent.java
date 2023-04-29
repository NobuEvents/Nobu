package com.nobu.event;


import java.io.Serializable;
import java.util.Arrays;

public class NobuEvent implements Serializable {


    private String type;

    private String schema;

    private Long timestamp;

    private String host;

    private Long offset;

    private byte[] message;

    public void setType(String type) {
        this.type = type;
    }

    public void setMessage(byte[] message) {
        this.message = message;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public String getType() {
        return type;
    }

    public byte[] getMessage() {
        return message;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public String getHost() {
        return host;
    }

    public Long getOffset() {
        return offset;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    @Override
    public String toString() {
        return String.format("Event{type=%s, message=%s, timestampNs=$d, host=%s, offset=%d}",
                type, Arrays.toString(message), timestamp, host, offset);
    }

    public void deepCopy(NobuEvent event) {
        this.type = event.getType();
        this.schema = event.getSchema();
        this.message = event.getMessage();
        this.timestamp = event.getTimestamp();
        this.host = event.getHost();
        this.offset = event.getOffset();
    }
}
