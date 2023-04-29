package com.nobu.exception;

public class RouteConfigException extends RuntimeException {
    public RouteConfigException(String message) {
        super(message);
    }

    public RouteConfigException(String message, Throwable cause) {
        super(message, cause);
    }

    public RouteConfigException(Throwable cause) {
        super(cause);
    }
}
