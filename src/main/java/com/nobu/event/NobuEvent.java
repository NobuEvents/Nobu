package com.nobu.event;


import java.io.Serializable;
import java.util.Arrays;

public class NobuEvent implements Serializable {


    private String type;

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

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Event{");
        sb.append("type='").append(type).append('\'');
        sb.append(", message=").append(Arrays.toString(message));
        sb.append(", timestampNs=").append(timestamp);
        sb.append(", host='").append(host).append('\'');
        sb.append(", offset=").append(offset);
        sb.append('}');
        return sb.toString();
    }
}
