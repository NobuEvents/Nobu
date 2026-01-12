package com.nobu.connect.sap.rest;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Builder for OData query strings.
 * Supports $filter, $orderby, $top, $skip, and other OData query options.
 */
public class ODataQueryBuilder {
    private final String baseUrl;
    private final List<String> filters;
    private String orderBy;
    private Integer top;
    private Integer skip;
    private String select;

    public ODataQueryBuilder(String baseUrl) {
        this.baseUrl = baseUrl;
        this.filters = new ArrayList<>();
    }

    /**
     * Add a filter condition.
     */
    public ODataQueryBuilder filter(String condition) {
        filters.add(condition);
        return this;
    }

    /**
     * Add a timestamp filter (greater than).
     */
    public ODataQueryBuilder filterTimestampGreaterThan(String fieldName, Instant timestamp) {
        String formatted = DateTimeFormatter.ISO_INSTANT.format(timestamp);
        filters.add(fieldName + " gt datetime'" + formatted + "'");
        return this;
    }

    /**
     * Set order by clause.
     */
    public ODataQueryBuilder orderBy(String fieldName) {
        this.orderBy = fieldName;
        return this;
    }

    /**
     * Set top (limit) clause.
     */
    public ODataQueryBuilder top(int count) {
        this.top = count;
        return this;
    }

    /**
     * Set skip (offset) clause.
     */
    public ODataQueryBuilder skip(int count) {
        this.skip = count;
        return this;
    }

    /**
     * Set select clause.
     */
    public ODataQueryBuilder select(String fields) {
        this.select = fields;
        return this;
    }

    /**
     * Build the OData query URL.
     */
    public String build() {
        StringBuilder url = new StringBuilder(baseUrl);
        List<String> queryParams = new ArrayList<>();

        if (!filters.isEmpty()) {
            String filterValue = String.join(" and ", filters);
            queryParams.add("$filter=" + encode(filterValue));
        }

        if (orderBy != null) {
            queryParams.add("$orderby=" + encode(orderBy));
        }

        if (top != null) {
            queryParams.add("$top=" + top);
        }

        if (skip != null) {
            queryParams.add("$skip=" + skip);
        }

        if (select != null) {
            queryParams.add("$select=" + encode(select));
        }

        if (!queryParams.isEmpty()) {
            url.append("?").append(String.join("&", queryParams));
        }

        return url.toString();
    }

    /**
     * URL encode a value.
     */
    private String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
