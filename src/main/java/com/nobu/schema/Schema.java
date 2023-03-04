package com.nobu.schema;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class Schema {

    private String type;
    private String description;
    private String owner;
    private String domain;
    @JsonProperty("compliance_owner")
    private String complianceOwner;
    private String channel;
    private String email;
    private String status;
    @JsonProperty("see_also")
    private List<String> seeAlso;
    private List<String> subscribers;
    private Map<String, Field> fields;
    @JsonProperty("tests")
    private Test test;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getComplianceOwner() {
        return complianceOwner;
    }

    public void setComplianceOwner(String complianceOwner) {
        this.complianceOwner = complianceOwner;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public List<String> getSeeAlso() {
        return seeAlso;
    }

    public void setSeeAlso(List<String> seeAlso) {
        this.seeAlso = seeAlso;
    }

    public List<String> getSubscribers() {
        return subscribers;
    }

    public void setSubscribers(List<String> subscribers) {
        this.subscribers = subscribers;
    }

    public Map<String, Field> getFields() {
        return fields;
    }

    public void setFields(Map<String, Field> fields) {
        this.fields = fields;
    }

    public Test getTest() {
        return test;
    }

    public void setTest(Test test) {
        this.test = test;
    }


    public static class Field {
        private String type;
        private String description;
        @JsonProperty("is_pii")
        private boolean isPii;
        private Key key;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public boolean isPii() {
            return isPii;
        }

        public void setPii(boolean pii) {
            isPii = pii;
        }

        public Key getKey() {
            return key;
        }

        public void setKey(Key key) {
            this.key = key;
        }
    }


    public static class Key {
        private String type;
        private String target;
        @JsonProperty("target_field")
        private String targetField;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getTarget() {
            return target;
        }

        public void setTarget(String target) {
            this.target = target;
        }

        public String getTargetField() {
            return targetField;
        }

        public void setTargetField(String targetField) {
            this.targetField = targetField;
        }
    }

    public static class Test {
        private String[] drop;
        private String[] dlq;
        private String[] warn;

        public String[] getDrop() {
            return drop;
        }

        public void setDrop(String[] value) {
            this.drop = value;
        }

        public String[] getDlq() {
            return dlq;
        }

        public void setDlq(String[] value) {
            this.dlq = value;
        }

        public String[] getWarn() {
            return warn;
        }

        public void setWarn(String[] value) {
            this.warn = value;
        }
    }


}
