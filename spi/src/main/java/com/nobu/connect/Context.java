package com.nobu.connect;

import java.util.Map;


public record Context(String type, String impl, Map<String, String> connectConfig, Map<String, String> routeConfig) {
}
