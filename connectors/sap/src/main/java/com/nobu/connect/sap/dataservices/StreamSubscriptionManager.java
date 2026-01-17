package com.nobu.connect.sap.dataservices;

import org.jboss.logging.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages stream subscriptions for SAP Data Services.
 * Handles both push and pull modes for replication streams.
 */
public class StreamSubscriptionManager {
    private static final Logger LOG = Logger.getLogger(StreamSubscriptionManager.class);
    
    private final HttpClient httpClient;
    private final String serverUrl;
    private final String subscriptionName;
    private final String replicationStream;
    private final String mode; // "push" or "pull"
    private final AtomicBoolean subscribed = new AtomicBoolean(false);
    private CompletableFuture<Void> pushListener;

    public StreamSubscriptionManager(String serverUrl, String subscriptionName, 
                                    String replicationStream, String mode) {
        this.serverUrl = serverUrl;
        this.subscriptionName = subscriptionName;
        this.replicationStream = replicationStream;
        this.mode = mode != null ? mode.toLowerCase() : "pull";
        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(30))
            .build();
    }

    /**
     * Subscribe to the replication stream.
     */
    public void subscribe() throws IOException {
        if (subscribed.compareAndSet(false, true)) {
            if ("push".equals(mode)) {
                subscribePushMode();
            } else {
                // Pull mode doesn't require explicit subscription
                LOG.info("Using pull mode - subscription not required");
            }
        }
    }

    /**
     * Subscribe in push mode (WebSocket or HTTP streaming).
     */
    private void subscribePushMode() throws IOException {
        try {
            // In a real implementation, this would establish a WebSocket connection
            // or HTTP streaming connection to receive pushed events
            String subscribeUrl = serverUrl + "/api/subscriptions/" + subscriptionName + "/subscribe";
            
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(subscribeUrl))
                .header("X-Replication-Stream", replicationStream)
                .POST(HttpRequest.BodyPublishers.noBody())
                .timeout(Duration.ofSeconds(30))
                .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200 && response.statusCode() != 201) {
                throw new IOException("Failed to subscribe to stream. Status: " + response.statusCode());
            }

            LOG.info("Subscribed to push stream: " + subscriptionName);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while subscribing", e);
        }
    }

    /**
     * Pull events from the stream (for pull mode).
     */
    public String pullEvents(int batchSize) throws IOException {
        try {
            String pullUrl = serverUrl + "/api/subscriptions/" + subscriptionName + "/pull";
            
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(pullUrl + "?batchSize=" + batchSize))
                .header("X-Replication-Stream", replicationStream)
                .GET()
                .timeout(Duration.ofSeconds(60))
                .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new IOException("Failed to pull events. Status: " + response.statusCode());
            }

            return response.body();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while pulling events", e);
        }
    }

    /**
     * Unsubscribe from the stream.
     */
    public void unsubscribe() {
        if (subscribed.compareAndSet(true, false)) {
            if ("push".equals(mode) && pushListener != null) {
                pushListener.cancel(true);
            }
            LOG.info("Unsubscribed from stream: " + subscriptionName);
        }
    }

    /**
     * Check if currently subscribed.
     */
    public boolean isSubscribed() {
        return subscribed.get();
    }
}
