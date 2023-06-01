package com.nobu.spi.event;


import java.io.Serializable;
import java.util.Arrays;


public class NobuEvent implements Serializable {

    /**
     * The Event Name is a unique identifier for a business action.
     * <p>
     * Event Name can have one-to-many {@link com.nobu.spi.connect.Connector} association.
     * The system will route the message to all the connector implementations associated with the Event Name.
     * </p>
     */
    private String eventName;

    /**
     * A unique identified for individual events. EventId is an optional field.
     */
    private String eventId;


    /**
     * Schema Resource Name (SRN) is a unique identifier for a schema.
     * <p>
     * The SRN is used to identify the schema of the message. We recommend SRN structure as
     * <code>srn:organization:domain:schemaName:schemaVersion</code>
     * (e.g.) <code>srn:nobu:core:user:1-0-0</code>.
     * </p>
     * <p>
     * The above example shows the SRN for the user schema. The schema version is 1-0-0. The schema belong to
     * the core domain of the nobu organization.
     * </p>
     *
     * <p>
     * The SRN field is an optional field. If there is no SRN defined, the system will not verify Data Quality Checks.
     * </p>
     */
    private String srn;

    /**
     * The timestamp of the message. Timestamp is in nanoseconds. Timestamp is an optional field.
     * <p>
     * Having the timestamp field will help the connectors to add event time processing logics.
     * </p>
     */
    private Long timestamp;

    /**
     * The host is the source of the message. Host is an optional field.
     */
    private String host;

    /**
     * The message is the actual payload of the event.
     */
    private String message;

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getEventName() {
        return eventName;
    }

    public String getMessage() {
        return message;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public String getHost() {
        return host;
    }

    public String getEventId() {
        return eventId;
    }

    public String getSrn() {
        return srn;
    }

    public void setSrn(String srn) {
        this.srn = srn;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NobuEvent{");
        sb.append("eventName='").append(eventName).append('\'');
        sb.append(", eventId='").append(eventId).append('\'');
        sb.append(", srn='").append(srn).append('\'');
        sb.append(", timestamp=").append(timestamp);
        sb.append(", host='").append(host).append('\'');
        sb.append(", message=").append(message);
        sb.append('}');
        return sb.toString();
    }

    public void deepCopy(NobuEvent event) {
        this.eventName = event.getEventName();
        this.srn = event.getSrn();
        this.message = event.getMessage();
        this.timestamp = event.getTimestamp();
        this.host = event.getHost();
        this.eventId = event.getEventId();
    }
}
