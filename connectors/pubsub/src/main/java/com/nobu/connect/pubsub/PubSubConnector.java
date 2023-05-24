package com.nobu.connect.pubsub;

import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import com.nobu.spi.connect.Connector;
import com.nobu.spi.connect.Context;
import com.nobu.spi.event.NobuEvent;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import org.jboss.logging.Logger;
import org.threeten.bp.Duration;


public class PubSubConnector implements Connector {

  private static final Logger LOG = Logger.getLogger(PubSubConnector.class.getName());

  protected static final String TOPIC_ID = "topic_id";
  protected static final String PROJECT_ID = "project_id";
  protected static final String CREDENTIALS_JSON = "credentials_json";
  protected static final String ORDERING_ENABLED = "ordering_enabled";
  protected static final String BATCHING_ENABLED = "batching_enabled";
  protected static final String BATCHING_DELAY_THRESHOLD = "batching_delay_threshold";
  protected static final String BATCHING_ELEMENT_COUNT_THRESHOLD = "batching_element_count_threshold";
  protected static final String BATCHING_REQUEST_BYTES_THRESHOLD = "batching_request_bytes_threshold";

  private Publisher publisher;

  @Override
  public void initialize(String target, Context context) {
    try {
      publisher = newPublisher(context);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Publisher newPublisher(Context context)
      throws IOException {
    var config = context.connectConfig();
    return Publisher.newBuilder(getTopicName(config))
        .setCredentialsProvider(getProvider(config.get(CREDENTIALS_JSON)))
        .setBatchingSettings(getBatchingSettings(config))
        .setEnableMessageOrdering(isOrderingEnabled(config))
        .build();
  }

  private static BatchingSettings getBatchingSettings(Map<String, String> config) {
    BatchingSettings.Builder builder = BatchingSettings.newBuilder();
    if (Boolean.parseBoolean(config.get(BATCHING_ENABLED))) {
      builder.setElementCountThreshold(Long.parseLong(config.get(BATCHING_ELEMENT_COUNT_THRESHOLD)));
      builder.setDelayThreshold(Duration.ofMillis(Long.parseLong(config.get(BATCHING_DELAY_THRESHOLD))));
      builder.setRequestByteThreshold(Long.parseLong(config.get(BATCHING_REQUEST_BYTES_THRESHOLD)));
    } else {
      builder.setIsEnabled(false);
    }
    return builder.build();
  }

  private static boolean isOrderingEnabled(Map<String, String> config) {
    return Boolean.parseBoolean(config.get(ORDERING_ENABLED));
  }

  private static TopicName getTopicName(Map<String, String> config) {
    return TopicName.of(config.get(PROJECT_ID), config.get(TOPIC_ID));
  }

  public CredentialsProvider getProvider(String credentialsJson) {
    try {
      GoogleCredentials credentials =
          ServiceAccountCredentials.fromStream(new FileInputStream(credentialsJson));
      return FixedCredentialsProvider.create(credentials);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onEvent(NobuEvent nobuEvent, long sequence, boolean endOfBatch)
      throws Exception {

    PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
        .setData(ByteString.copyFrom(nobuEvent.getMessage()))
        .putAttributes("type", nobuEvent.getEventName())
        .putAttributes("schema", nobuEvent.getSrn() == null ? "" : nobuEvent.getSrn())
        .putAttributes("timestamp", nobuEvent.getTimestamp() == null ? "" : nobuEvent.getTimestamp().toString())
        .putAttributes("host", nobuEvent.getHost() == null ? "" : nobuEvent.getHost())
        .putAttributes("offset", nobuEvent.getEventId() == null ? "" : nobuEvent.getEventId())
        .putAttributes("sequence", String.valueOf(sequence))
        .build();
    publisher.publish(pubsubMessage);
  }

  @Override
  public void shutdown() {
    if (publisher != null) {
      try {
        publisher.publishAllOutstanding();
        publisher.shutdown();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
