package com.nobu.spi.event;


import java.io.Serializable;
import java.util.Arrays;


public class NobuEvent implements Serializable {


    private String routerId;

    private String schema;

    private Long timestamp;

    private String host;

    private Long offset;

    private byte[] message;

    public void setRouterId(String routerId) {
        this.routerId = routerId;
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

    public String getRouterId() {
        return routerId;
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
            routerId, Arrays.toString(message), timestamp, host, offset);
    }

    public void deepCopy(NobuEvent event) {
        this.routerId = event.getRouterId();
        this.schema = event.getSchema();
        this.message = event.getMessage();
        this.timestamp = event.getTimestamp();
        this.host = event.getHost();
        this.offset = event.getOffset();
    }
}
