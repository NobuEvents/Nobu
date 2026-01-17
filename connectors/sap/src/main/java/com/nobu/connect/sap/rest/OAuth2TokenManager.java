package com.nobu.connect.sap.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;

/**
 * Manages OAuth2 token acquisition and refresh for SAP REST API authentication.
 */
public class OAuth2TokenManager {
    private static final Logger LOG = Logger.getLogger(OAuth2TokenManager.class);
    
    private final String tokenUrl;
    private final String clientId;
    private final String clientSecret;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    
    private volatile String accessToken;
    private volatile Instant tokenExpiry;
    private final long tokenRefreshBufferSeconds = 60; // Refresh 60 seconds before expiry
    private final Object refreshLock = new Object(); // Separate lock for token refresh

    // Shared HttpClient for better connection pooling
    private static final HttpClient SHARED_HTTP_CLIENT = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .build();

    public OAuth2TokenManager(String tokenUrl, String clientId, String clientSecret) {
        this.tokenUrl = tokenUrl;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.httpClient = SHARED_HTTP_CLIENT;
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Get a valid access token, refreshing if necessary.
     * Uses double-checked locking pattern for better performance.
     */
    public String getAccessToken() throws IOException {
        // Fast path - check if token is valid without locking
        String currentToken = accessToken;
        if (currentToken != null && !isTokenExpired()) {
            return currentToken;
        }
        
        // Slow path - need to refresh token
        synchronized (refreshLock) {
            // Double-check after acquiring lock
            if (accessToken != null && !isTokenExpired()) {
                return accessToken;
            }
            refreshToken();
            return accessToken;
        }
    }

    /**
     * Check if the current token is expired or about to expire.
     * Thread-safe - reads volatile field.
     */
    private boolean isTokenExpired() {
        Instant expiry = tokenExpiry;
        if (expiry == null) {
            return true;
        }
        return Instant.now().plusSeconds(tokenRefreshBufferSeconds).isAfter(expiry);
    }

    /**
     * Refresh the OAuth2 access token.
     */
    private void refreshToken() throws IOException {
        try {
            String credentials = Base64.getEncoder().encodeToString(
                (clientId + ":" + clientSecret).getBytes()
            );

            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(tokenUrl))
                .header("Authorization", "Basic " + credentials)
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString("grant_type=client_credentials"))
                .timeout(Duration.ofSeconds(30))
                .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new IOException("Failed to obtain OAuth2 token. Status: " + response.statusCode() + 
                    ", Body: " + response.body());
            }

            JsonNode jsonResponse = objectMapper.readTree(response.body());
            accessToken = jsonResponse.get("access_token").asText();
            
            // Calculate expiry time (default to 3600 seconds if not provided)
            int expiresIn = jsonResponse.has("expires_in") 
                ? jsonResponse.get("expires_in").asInt() 
                : 3600;
            tokenExpiry = Instant.now().plusSeconds(expiresIn - tokenRefreshBufferSeconds);

            LOG.debug("OAuth2 token refreshed successfully. Expires at: " + tokenExpiry);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while refreshing OAuth2 token", e);
        }
    }

    /**
     * Clear the current token (force refresh on next request).
     */
    public void clearToken() {
        synchronized (refreshLock) {
            accessToken = null;
            tokenExpiry = null;
        }
    }
}
