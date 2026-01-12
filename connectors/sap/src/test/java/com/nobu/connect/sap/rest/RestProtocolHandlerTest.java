package com.nobu.connect.sap.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nobu.connect.sap.model.ChangeRecord;
import com.nobu.connect.sap.model.ChangeType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class RestProtocolHandlerTest {

    private RestProtocolHandler protocolHandler;
    private OAuth2TokenManager mockTokenManager;
    private Map<String, String> config;

    @BeforeEach
    public void setUp() {
        mockTokenManager = mock(OAuth2TokenManager.class);
        // Use testing constructor to inject mock token manager
        // Pass null for HttpClient as we don't mock it here yet (initialized in handler)
        protocolHandler = new RestProtocolHandler(null, mockTokenManager);
        
        config = new HashMap<>();
        config.put("rest.base.url", "https://test.sap.com/odata");
        config.put("rest.service", "TestService");
        config.put("rest.entity.set", "ChangeLogs");
        config.put("auth.type", "oauth2");
        config.put("oauth2.token.url", "https://test.sap.com/oauth/token");
        config.put("oauth2.client.id", "test_client");
        config.put("oauth2.client.secret", "test_secret");
    }

    @Test
    public void testInitialize() throws Exception {
        when(mockTokenManager.getAccessToken()).thenReturn("mock_token");
        assertDoesNotThrow(() -> protocolHandler.initialize(config));
        assertTrue(protocolHandler.isHealthy());
    }

    @Test
    public void testInitializeMissingBaseUrl() {
        config.remove("rest.base.url");
        
        assertThrows(IllegalArgumentException.class, () -> {
            protocolHandler.initialize(config);
        });
    }

    @Test
    public void testInitializeMissingOAuth2Config() {
        config.remove("oauth2.token.url");
        
        assertThrows(IllegalArgumentException.class, () -> {
            protocolHandler.initialize(config);
        });
    }

    @Test
    public void testIsHealthyNotInitialized() {
        assertFalse(protocolHandler.isHealthy());
    }

    @Test
    public void testShutdown() {
        protocolHandler.initialize(config);
        assertDoesNotThrow(() -> protocolHandler.shutdown());
    }

    @Test
    public void testPollChangesNotInitialized() {
        assertThrows(IllegalStateException.class, () -> {
            protocolHandler.pollChanges(0L, 100);
        });
    }

    @Test
    public void testPerformJoinNotInitialized() {
        assertThrows(IllegalStateException.class, () -> {
            protocolHandler.performJoin("table", "key", "value");
        });
    }
}
