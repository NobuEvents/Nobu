package com.nobu.connect.kafka;

import com.nobu.connect.Connector;
import com.nobu.event.NobuEvent;
import com.nobu.route.RouteFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.jboss.logging.Logger;

import java.util.Map;

public class KafkaConnector implements Connector {

    private static final Logger LOG = Logger.getLogger(KafkaConnector.class);

    private KafkaProducer<String, byte[]> producer;
    private String topic;

    private boolean sendHeaders = false;

    @Override
    public void initialize(String target, RouteFactory.Connection connection, Map<String, String> routeConfig) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connection.getConfig().get("bootstrap_server"));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "1048576");

        producer = new KafkaProducer<>(properties);
        topic = routeConfig.get("topic");
        sendHeaders = Boolean.parseBoolean(routeConfig.get("send_headers"));
    }



    @Override
    public void onEvent(NobuEvent event, long sequence, boolean endOfBatch) throws Exception {
      List<Future<RecordMetadata>> futures = new ArrayList<>();

      ProducerRecord<String, byte[]> record;
      if (sendHeaders) {
        List<Header> headers = new ArrayList<>();
        headers.add(new RecordHeader("headerKey", "headerValue".getBytes()));
        record = new ProducerRecord<>(topic, 2, event.getType(), event.getMessage(), headers);
      } else {
        record = new ProducerRecord<>(topic, event.getMessage());
      }
      futures.add(producer.send(record));
      producer.flush();
    }

  @Override
  public void shutdown() {
    producer.close();
  }
}
