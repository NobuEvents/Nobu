package com.nobu.connect;

import com.nobu.connect.kafka.KafkaConnector;
import com.nobu.event.NobuEvent;
import com.nobu.route.RouteFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;


public class KafkaConnectorTest {

    private static final String TOPIC = "my-topic";
    private static final boolean SEND_HEADERS = true;

    private KafkaConnector kafkaConnector;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        kafkaConnector = new KafkaConnector();
    }

    @AfterEach
    void tearDown() {
        kafkaConnector.shutdown();
    }

    private void initialize(String bootstrapServers) {
        Map<String, String> routeConfig = new HashMap<>();
        routeConfig.put(KafkaConnector.TOPIC, TOPIC);
        routeConfig.put(KafkaConnector.SEND_HEADERS, Boolean.toString(SEND_HEADERS));

        RouteFactory.Connection connection = mock(RouteFactory.Connection.class);
        Map<String, String> connectionConfig = new HashMap<>();
        connectionConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        connectionConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        connectionConfig.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        connectionConfig.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        connectionConfig.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "1048576");
        when(connection.getConfig()).thenReturn(connectionConfig);

        kafkaConnector.initialize("target", connection, routeConfig);

    }

    @Test
    void testInitializeWithValidBootstrapServers() {
        initialize("localhost:9092");
        assertEquals(TOPIC, kafkaConnector.getTopic());

    }

    @Test
    void testInitializeWithInValidBootstrapServers() {
        assertThrows(KafkaException.class, () -> initialize("unknown.server:9092"));
    }

    @Test
    void testShutdown() {
        assertDoesNotThrow(() -> kafkaConnector.shutdown());
    }

    @Test
    void testPublishEvent() throws Exception {
        // Step 1: Mock the KafkaProducer
        KafkaProducer<String, byte[]> producer = mock(KafkaProducer.class);
        Future<RecordMetadata> future = mock(Future.class);
        when(producer.send(any(ProducerRecord.class))).thenReturn(future);

        // Step 2: Mock the KafkaConnector
        KafkaConnector connector = mock(KafkaConnector.class);
        when(connector.getProducer()).thenReturn(producer);
        when(connector.getTopic()).thenReturn("my-topic");

        // Step 3: Mock the KafkaConnector.publishEvent() method
        doCallRealMethod().when(connector).publishEvent(
                any(NobuEvent.class), anyLong(), anyBoolean(),
                anyBoolean(), any(List.class), any(KafkaProducer.class));

        // Step 4: initialize the record variable to test the publishEvent() method
        List<ProducerRecord<String, byte[]>> records = new ArrayList<>();

        NobuEvent event1 = new NobuEvent();
        event1.setType("type1");
        event1.setMessage("message1".getBytes());
        NobuEvent event2 = new NobuEvent();
        event2.setType("type1");
        event2.setMessage("message2".getBytes());

        // Publish a couple of message with endOfBatch=false
        connector.publishEvent(event1, 0, false, false, records, producer);
        connector.publishEvent(event2, 0, false, false, records, producer);
        assertEquals(2, records.size());

        // Publish a couple of message with endOfBatch=true. It should flush the records into Kafka
        connector.publishEvent(event1, 1, true, false, records, producer);
        assertEquals(0, records.size());

    }


}
