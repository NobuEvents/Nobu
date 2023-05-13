package com.nobu.spi.connect.pubsub;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.nobu.spi.connect.Context;
import com.nobu.event.NobuEvent;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


@ExtendWith(MockitoExtension.class)
public class PubSubConnectorTest {

  private Context context;
  @InjectMocks
  private PubSubConnector pubSubConnector;

  @Mock
  private Publisher publisher;

  @BeforeEach
  void setUp()
      throws IOException {
    Map<String, String> connectionConfig = new HashMap<>();
    connectionConfig.put(PubSubConnector.PROJECT_ID, "test-project");
    connectionConfig.put(PubSubConnector.TOPIC_ID, "test-topic");
    connectionConfig.put(PubSubConnector.CREDENTIALS_JSON, getCredentialsJsonPath());
    connectionConfig.put(PubSubConnector.ORDERING_ENABLED, "true");
    connectionConfig.put(PubSubConnector.BATCHING_ENABLED, "true");
    connectionConfig.put(PubSubConnector.BATCHING_DELAY_THRESHOLD, "100");
    connectionConfig.put(PubSubConnector.BATCHING_ELEMENT_COUNT_THRESHOLD, "100");
    connectionConfig.put(PubSubConnector.BATCHING_REQUEST_BYTES_THRESHOLD, "1000");
    context = new Context("PubSub", "PubSub", connectionConfig, Map.of());
  }

  @Test()
  void initialize()
      throws IOException {
    assertThrows(RuntimeException.class, () -> pubSubConnector.initialize("test-target", context));
  }

  @Test
  void onEvent()
      throws Exception {
    NobuEvent nobuEvent = new NobuEvent();
    nobuEvent.setType("type1");
    nobuEvent.setMessage("message1".getBytes());

    pubSubConnector.onEvent(nobuEvent, 0, false);

    PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
        .setData(ByteString.copyFrom(nobuEvent.getMessage()))
        .putAttributes("type", nobuEvent.getType())
        .putAttributes("schema", "")
        .putAttributes("timestamp", "")
        .putAttributes("host", "")
        .putAttributes("offset", "")
        .putAttributes("sequence", "0")
        .build();
    verify(publisher, times(1)).publish(pubsubMessage);
  }

  @Test
  void shutdown()
      throws Exception {
    pubSubConnector.shutdown();

    verify(publisher, times(1)).publishAllOutstanding();
    verify(publisher, times(1)).shutdown();
  }

  @Test
  void shutdownThrowsException()
      throws Exception {
    try {
      pubSubConnector.shutdown();
    } catch (RuntimeException e) {
      assert (e.getCause() instanceof Exception);
    }
    verify(publisher, times(1)).publishAllOutstanding();
    verify(publisher, times(1)).shutdown();
  }

  private static String getCredentialsJsonPath() {
    Path resourceDirectory = Paths.get("src", "test", "resources");
    String absolutePath = resourceDirectory.toFile().getAbsolutePath();
    return absolutePath + "/credentials.json";
  }
}
