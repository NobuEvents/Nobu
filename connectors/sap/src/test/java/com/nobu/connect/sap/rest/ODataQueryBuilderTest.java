package com.nobu.connect.sap.rest;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

public class ODataQueryBuilderTest {

    @Test
    public void testBuildBasicQuery() {
        ODataQueryBuilder builder = new ODataQueryBuilder("https://test.com/api/EntitySet");
        String query = builder.build();
        
        assertEquals("https://test.com/api/EntitySet", query);
    }

    @Test
    public void testBuildQueryWithFilter() {
        ODataQueryBuilder builder = new ODataQueryBuilder("https://test.com/api/EntitySet")
            .filter("Name eq 'Test'");
        
        String query = builder.build();
        assertTrue(query.contains("$filter"));
        // URL encoded check: space -> + or %20
        assertTrue(query.contains("Name+eq+%27Test%27") || query.contains("Name%20eq%20%27Test%27"));
    }

    @Test
    public void testBuildQueryWithMultipleFilters() {
        ODataQueryBuilder builder = new ODataQueryBuilder("https://test.com/api/EntitySet")
            .filter("Name eq 'Test'")
            .filter("Status eq 'Active'");
        
        String query = builder.build();
        assertTrue(query.contains("$filter"));
        assertTrue(query.contains("+and+") || query.contains("%20and%20"));
    }

    @Test
    public void testBuildQueryWithTimestampFilter() {
        Instant timestamp = Instant.now();
        ODataQueryBuilder builder = new ODataQueryBuilder("https://test.com/api/EntitySet")
            .filterTimestampGreaterThan("Timestamp", timestamp);
        
        String query = builder.build();
        assertTrue(query.contains("$filter"));
        assertTrue(query.contains("Timestamp"));
        assertTrue(query.contains("gt"));
    }

    @Test
    public void testBuildQueryWithOrderBy() {
        ODataQueryBuilder builder = new ODataQueryBuilder("https://test.com/api/EntitySet")
            .orderBy("Timestamp");
        
        String query = builder.build();
        assertTrue(query.contains("$orderby"));
        assertTrue(query.contains("Timestamp"));
    }

    @Test
    public void testBuildQueryWithTop() {
        ODataQueryBuilder builder = new ODataQueryBuilder("https://test.com/api/EntitySet")
            .top(100);
        
        String query = builder.build();
        assertTrue(query.contains("$top"));
        assertTrue(query.contains("100"));
    }

    @Test
    public void testBuildQueryWithSkip() {
        ODataQueryBuilder builder = new ODataQueryBuilder("https://test.com/api/EntitySet")
            .skip(50);
        
        String query = builder.build();
        assertTrue(query.contains("$skip"));
        assertTrue(query.contains("50"));
    }

    @Test
    public void testBuildQueryWithSelect() {
        ODataQueryBuilder builder = new ODataQueryBuilder("https://test.com/api/EntitySet")
            .select("Id,Name,Status");
        
        String query = builder.build();
        assertTrue(query.contains("$select"));
        assertTrue(query.contains("Id%2CName%2CStatus"));
    }

    @Test
    public void testBuildQueryWithAllOptions() {
        Instant timestamp = Instant.now();
        ODataQueryBuilder builder = new ODataQueryBuilder("https://test.com/api/EntitySet")
            .filter("Status eq 'Active'")
            .filterTimestampGreaterThan("Timestamp", timestamp)
            .orderBy("Timestamp")
            .top(100)
            .skip(0)
            .select("Id,Name");
        
        String query = builder.build();
        assertTrue(query.contains("$filter"));
        assertTrue(query.contains("$orderby"));
        assertTrue(query.contains("$top"));
        assertTrue(query.contains("$skip"));
        assertTrue(query.contains("$select"));
    }

    @Test
    public void testBuildQueryWithSpecialCharacters() {
        ODataQueryBuilder builder = new ODataQueryBuilder("https://test.com/api/EntitySet")
            .filter("Name eq 'Test & Value'");
        
        String query = builder.build();
        assertTrue(query.contains("$filter"));
        // Should be URL encoded
        assertTrue(query.contains("Test"));
    }
}
