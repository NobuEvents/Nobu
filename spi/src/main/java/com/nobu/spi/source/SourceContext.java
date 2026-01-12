package com.nobu.spi.source;

import java.util.Map;

/**
 * Context information for source connectors.
 * Contains configuration and routing information needed to initialize a source connector.
 */
public record SourceContext(
    String type,
    String impl,
    Map<String, String> sourceConfig,
    Map<String, String> routeConfig
) {
}
