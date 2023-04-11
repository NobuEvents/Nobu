package com.nobu.schema;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.nobu.config.ConfigReader;
import com.nobu.config.YamlReader;

import java.util.List;
import java.util.Map;
import java.util.Optional;


@JsonIgnoreProperties(ignoreUnknown = true)
public class SchemaFactory {

    @JsonProperty("schemas")
    private Map<String, Schema> schemaMap;

    public Map<String, Schema> getSchemaMap() {
        return schemaMap;
    }

    public void setSchemaMap(Map<String, Schema> schemaMap) {
        this.schemaMap = schemaMap;
    }


    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Schema {

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
        private Map<String, Test> test;

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

        public Map<String, Test> getTest() {
            return test;
        }

        public void setTest(Map<String, Test> test) {
            this.test = test;
        }


        @JsonIgnoreProperties(ignoreUnknown = true)
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


    }


    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Field {
        private String type;
        private String description;
        @JsonProperty("is_pii")
        private boolean isPii;
        private Schema.Key key;

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

        public Schema.Key getKey() {
            return key;
        }

        public void setKey(Schema.Key key) {
            this.key = key;
        }
    }


    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Test {
        private String name;
        private String query;
        private String description;
        private String target;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getQuery() {
            return query;
        }

        public void setQuery(String query) {
            this.query = query;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public String getTarget() {
            return target;
        }

        public void setTarget(String target) {
            this.target = target;
        }
    }

    @Override
    public String toString() {
        return "SchemaFactory{" + "schemas=" + schemaMap + '}';
    }

    @JsonIgnore
    public static Optional<SchemaFactory> get(String schemaFile) {
        try {
            return Optional.of(YamlReader.getYaml(ConfigReader.getRouteConfig(schemaFile), SchemaFactory.class));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }
}
