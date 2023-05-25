package com.nobu.connect.kafka;

import com.nobu.spi.connect.Connector;
import com.nobu.spi.connect.Context;
import com.nobu.spi.event.NobuEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import static org.apache.kafka.clients.producer.ProducerConfig.*;


public class KafkaConnector implements Connector {

  public static final String TOPIC = "topic";
  public static final String SEND_HEADERS = "send_headers";
  private static final String CONNECTOR_KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
  private static final String CONNECTOR_VALUE_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer";

  private static final String DEFAULT_BATCH_SIZE = "16384";
  private static final String DEFAULT_LINGER_MS = "5";
  private static final String DEFAULT_MAX_REQUEST_SIZE = "1048576";

  private KafkaProducer<String, byte[]> producer;
  private String topic;

  private List<ProducerRecord<String, byte[]>> records;

  private boolean sendHeaders = false;

  public KafkaProducer<String, byte[]> getProducer() {
    return producer;
  }

  public String getTopic() {
    return topic;
  }

  public List<ProducerRecord<String, byte[]>> getRecords() {
    return records;
  }

  @Override
  public void initialize(String target, Context context) {
    producer = newKafkaProducer(context);
    topic = context.routeConfig().get(TOPIC);
    sendHeaders = Boolean.parseBoolean(context.routeConfig().get(SEND_HEADERS));
    records = new ArrayList<>();
  }

  public KafkaProducer<String, byte[]> newKafkaProducer(Context context) {
    Properties properties = new Properties();
    var connectConfig = context.connectConfig();

    properties.put(BOOTSTRAP_SERVERS_CONFIG, connectConfig.get(BOOTSTRAP_SERVERS_CONFIG));
    properties.put(KEY_SERIALIZER_CLASS_CONFIG, CONNECTOR_KEY_SERIALIZER);
    properties.put(VALUE_SERIALIZER_CLASS_CONFIG, CONNECTOR_VALUE_SERIALIZER);
    properties.put(ACKS_CONFIG, connectConfig.getOrDefault(ACKS_CONFIG, "all"));
    properties.put(BATCH_SIZE_CONFIG, connectConfig.getOrDefault(BATCH_SIZE_CONFIG, DEFAULT_BATCH_SIZE));
    properties.put(LINGER_MS_CONFIG, connectConfig.getOrDefault(LINGER_MS_CONFIG, DEFAULT_LINGER_MS));
    properties.put(MAX_REQUEST_SIZE_CONFIG,
        connectConfig.getOrDefault(MAX_REQUEST_SIZE_CONFIG, DEFAULT_MAX_REQUEST_SIZE));
    return new KafkaProducer<>(properties);
  }

  @Override
  public void onEvent(NobuEvent event, long sequence, boolean endOfBatch)
      throws Exception {

    publishEvent(event, sequence, endOfBatch, sendHeaders, getRecords(), getProducer());
  }

  private void publishEvent(NobuEvent event, long sequence, boolean endOfBatch, boolean sendHeaders,
      List<ProducerRecord<String, byte[]>> records,
      KafkaProducer<String, byte[]> producer) {
    ProducerRecord<String, byte[]> record;
    if (sendHeaders) {
      List<Header> headers = new ArrayList<>();
      headers.add(new RecordHeader("type", event.getEventName().getBytes()));
      headers.add(new RecordHeader("schema", event.getSrn().getBytes()));
      headers.add(new RecordHeader("timestamp", event.getTimestamp().toString().getBytes()));
      headers.add(new RecordHeader("host", event.getHost().getBytes()));
      headers.add(new RecordHeader("offset", event.getEventId().getBytes()));
      headers.add(new RecordHeader("sequence", String.valueOf(sequence).getBytes()));
      record = new ProducerRecord<>(getTopic(), null, event.getEventName(), event.getMessage(), headers);
    } else {
      record = new ProducerRecord<>(getTopic(), event.getMessage());
    }
    records.add(record);
    if (endOfBatch) {
      records.forEach(producer::send);
      producer.flush();
      records.clear();
    }
  }

  @Override
  public void shutdown() {
    if (producer != null) {
      producer.flush();
      producer.close();
    }
  }
}
