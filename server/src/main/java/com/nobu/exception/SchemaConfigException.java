package com.nobu.exception;

public class SchemaConfigException extends RuntimeException {


    public SchemaConfigException(String message) {
        super(message);
    }

    public SchemaConfigException(String message, Throwable cause) {
        super(message, cause);
    }
}
