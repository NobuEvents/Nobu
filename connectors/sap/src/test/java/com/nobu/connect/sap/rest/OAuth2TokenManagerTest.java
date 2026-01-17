package com.nobu.connect.sap.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.mockito.ArgumentMatchers;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class OAuth2TokenManagerTest {

    private OAuth2TokenManager tokenManager;
    private HttpClient mockHttpClient;
    @SuppressWarnings("unchecked")
    private HttpResponse<String> mockResponse;
    private ObjectMapper objectMapper;

    @BeforeEach
    public void setUp() throws Exception {
        mockHttpClient = mock(HttpClient.class);
        @SuppressWarnings("unchecked")
        HttpResponse<String> response = mock(HttpResponse.class);
        mockResponse = response;
        objectMapper = new ObjectMapper();
        
        // Use reflection to inject mock HttpClient
        tokenManager = new OAuth2TokenManager(
            "https://test.sap.com/oauth/token",
            "test_client",
            "test_secret"
        );
        
        // Inject mock client
        java.lang.reflect.Field httpClientField = OAuth2TokenManager.class.getDeclaredField("httpClient");
        httpClientField.setAccessible(true);
        httpClientField.set(tokenManager, mockHttpClient);
    }

    @Test
    public void testGetAccessTokenSuccess() throws Exception {
        String tokenResponse = "{\"access_token\":\"test_token_123\",\"expires_in\":3600}";
        
        when(mockHttpClient.send(any(HttpRequest.class), ArgumentMatchers.<HttpResponse.BodyHandler<String>>any()))
            .thenReturn(mockResponse);
        when(mockResponse.statusCode()).thenReturn(200);
        when(mockResponse.body()).thenReturn(tokenResponse);

        String token = tokenManager.getAccessToken();

        assertNotNull(token);
        assertEquals("test_token_123", token);
    }

    @Test
    public void testGetAccessTokenWithExpiry() throws Exception {
        String tokenResponse = "{\"access_token\":\"test_token_123\",\"expires_in\":3600}";
        
        when(mockHttpClient.send(any(HttpRequest.class), ArgumentMatchers.<HttpResponse.BodyHandler<String>>any()))
            .thenReturn(mockResponse);
        when(mockResponse.statusCode()).thenReturn(200);
        when(mockResponse.body()).thenReturn(tokenResponse);

        String token1 = tokenManager.getAccessToken();
        String token2 = tokenManager.getAccessToken();

        // Should return same token without refreshing
        assertEquals(token1, token2);
        verify(mockHttpClient, times(1)).send(any(HttpRequest.class), ArgumentMatchers.<HttpResponse.BodyHandler<String>>any());
    }

    @Test
    public void testGetAccessTokenRefreshOnExpiry() throws Exception {
        String tokenResponse1 = "{\"access_token\":\"token1\",\"expires_in\":1}";
        String tokenResponse2 = "{\"access_token\":\"token2\",\"expires_in\":3600}";
        
        when(mockHttpClient.send(any(HttpRequest.class), ArgumentMatchers.<HttpResponse.BodyHandler<String>>any()))
            .thenReturn(mockResponse);
        when(mockResponse.statusCode()).thenReturn(200);
        when(mockResponse.body())
            .thenReturn(tokenResponse1)
            .thenReturn(tokenResponse2);

        String token1 = tokenManager.getAccessToken();
        
        // Wait for token to expire
        Thread.sleep(1100);
        
        String token2 = tokenManager.getAccessToken();

        assertEquals("token1", token1);
        assertEquals("token2", token2);
        verify(mockHttpClient, times(2)).send(any(HttpRequest.class), ArgumentMatchers.<HttpResponse.BodyHandler<String>>any());
    }

    @Test
    public void testGetAccessTokenWithHttpError() throws Exception {
        when(mockHttpClient.send(any(HttpRequest.class), ArgumentMatchers.<HttpResponse.BodyHandler<String>>any()))
            .thenReturn(mockResponse);
        when(mockResponse.statusCode()).thenReturn(401);
        when(mockResponse.body()).thenReturn("Unauthorized");

        assertThrows(IOException.class, () -> {
            tokenManager.getAccessToken();
        });
    }

    @Test
    public void testGetAccessTokenConcurrentAccess() throws Exception {
        String tokenResponse = "{\"access_token\":\"test_token\",\"expires_in\":3600}";
        
        when(mockHttpClient.send(any(HttpRequest.class), ArgumentMatchers.<HttpResponse.BodyHandler<String>>any()))
            .thenReturn(mockResponse);
        when(mockResponse.statusCode()).thenReturn(200);
        when(mockResponse.body()).thenReturn(tokenResponse);

        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    String token = tokenManager.getAccessToken();
                    assertNotNull(token);
                } catch (Exception e) {
                    fail("Exception in thread: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        
        // Should only refresh token once due to double-checked locking
        verify(mockHttpClient, atMost(threadCount)).send(any(HttpRequest.class), ArgumentMatchers.<HttpResponse.BodyHandler<String>>any());
        
        executor.shutdown();
    }

    @Test
    public void testClearToken() throws Exception {
        String tokenResponse = "{\"access_token\":\"test_token\",\"expires_in\":3600}";
        
        when(mockHttpClient.send(any(HttpRequest.class), ArgumentMatchers.<HttpResponse.BodyHandler<String>>any()))
            .thenReturn(mockResponse);
        when(mockResponse.statusCode()).thenReturn(200);
        when(mockResponse.body()).thenReturn(tokenResponse);

        String token1 = tokenManager.getAccessToken();
        tokenManager.clearToken();
        String token2 = tokenManager.getAccessToken();

        assertEquals("test_token", token1);
        assertEquals("test_token", token2);
        verify(mockHttpClient, times(2)).send(any(HttpRequest.class), ArgumentMatchers.<HttpResponse.BodyHandler<String>>any());
    }

    @Test
    public void testGetAccessTokenWithInterruptedException() throws Exception {
        when(mockHttpClient.send(any(HttpRequest.class), ArgumentMatchers.<HttpResponse.BodyHandler<String>>any()))
            .thenThrow(new InterruptedException("Interrupted"));

        assertThrows(IOException.class, () -> {
            tokenManager.getAccessToken();
        });
        
        assertTrue(Thread.currentThread().isInterrupted());
    }
}
