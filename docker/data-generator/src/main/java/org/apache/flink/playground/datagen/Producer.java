package org.apache.flink.playground.datagen;

import java.time.ZoneOffset;
import java.util.Properties;
import org.apache.flink.playground.datagen.model.StructureDataSerializer;
import org.apache.flink.playground.datagen.model.StructureDataSupplier;
import org.apache.flink.playground.datagen.model.StructuredData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Generates CSV transaction records at a rate */
public class Producer implements Runnable, AutoCloseable {
  private Logger log = LoggerFactory.getLogger(Producer.class);

  private volatile boolean isRunning;

  private final String brokers;

  private final String topic;

  public Producer(String brokers, String topic) {
    this.brokers = brokers;
    this.topic = topic;
    this.isRunning = true;
  }

  @Override
  public void run() {
    KafkaProducer<String, StructuredData> producer = new KafkaProducer<>(getProperties());

    Throttler throttler = new Throttler(100);

    StructureDataSupplier structureDataSupplier = new StructureDataSupplier();

    while (isRunning) {

      StructuredData structuredData = structureDataSupplier.get();
      System.out.println("[🤮data]:" + structuredData);

      long millis = structuredData.timestamp.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();

      ProducerRecord<String, StructuredData> record =
          new ProducerRecord<>(topic, null, millis, structuredData.uuid, structuredData);
      producer.send(record);

      try {
        throttler.throttle();
      } catch (InterruptedException e) {
        isRunning = false;
      }
    }

    producer.close();
  }

  @Override
  public void close() {
    isRunning = false;
  }

  private Properties getProperties() {
    final Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.RETRIES_CONFIG, 0);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StructureDataSerializer.class);
    return props;
  }
}
