package com.nobu.spi.event;


import java.io.Serializable;
import java.util.Arrays;


public class NobuEvent implements Serializable {

    /**
     * The router id is a unique identifier for the router.
     * <p>
     *     RouterId can have one-to-many {@link com.nobu.spi.connect.Connector} association.
     *     The system will route the message to all the connector implementations associated with the router id.
     * </p>
     */
    private String routerId;

    /**
     * Schema Resource Name (SRN) is a unique identifier for a schema.
     * <p>
     *     The SRN is used to identify the schema of the message. We recommend SRN structure as
     *     <code>srn:organization:domain:schemaName:schemaVersion</code>
     *     (e.g.) <code>srn:nobu:core:user:1-0-0</code>.
     * </p>
     * <p>
     *     The above example shows the SRN for the user schema. The schema version is 1-0-0. The schema belong to
     *     the core domain of the nobu organization.
     * </p>
     *
     * <p>
     *     The SRN field is an optional field. If there is no SRN defined, the system will not verify Data Quality Checks.
     * </p>
     *
     */
    private String srn;

    /**
     * The timestamp of the message. Timestamp is in nanoseconds. Timestamp is an optional field.
     * <p>
     *     Having the timestamp field will help the connectors to add event time processing logics.
     * </p>
     */
    private Long timestamp;

    /**
     * The host is the source of the message. Host is an optional field.
     */
    private String host;

    /**
     * The offset is the position of the message in the source. Offset is an optional field.
     */
    private Long offset;

    /**
     * The message is the actual payload of the event.
     */
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

    public String getSrn() {
        return srn;
    }

    public void setSrn(String srn) {
        this.srn = srn;
    }

    @Override
    public String toString() {
        return String.format("Event{type=%s, message=%s, timestampNs=$d, host=%s, offset=%d}",
            routerId, Arrays.toString(message), timestamp, host, offset);
    }

    public void deepCopy(NobuEvent event) {
        this.routerId = event.getRouterId();
        this.srn = event.getSrn();
        this.message = event.getMessage();
        this.timestamp = event.getTimestamp();
        this.host = event.getHost();
        this.offset = event.getOffset();
    }
}
