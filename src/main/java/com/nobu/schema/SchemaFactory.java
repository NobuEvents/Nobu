package com.nobu.schema;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.nobu.config.ConfigReader;
import com.nobu.config.YamlReader;

import java.util.Map;
import java.util.Optional;

public class SchemaFactory {

    @JsonProperty("schemas")
    private Map<String, Schema> schemaMap;

    public Map<String, Schema> getSchemaMap() {
        return schemaMap;
    }

    public void setSchemaMap(Map<String, Schema> schemaMap) {
        this.schemaMap = schemaMap;
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
