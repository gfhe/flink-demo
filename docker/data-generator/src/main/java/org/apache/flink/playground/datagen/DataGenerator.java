package org.apache.flink.playground.datagen;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A basic data generator for continuously writing data into a Kafka topic. */
public class DataGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(DataGenerator.class);

  private static final String KAFKA = "kafka:9092";

  private static final String TOPIC = "transactions";

  public static void main(String[] args) {
    Producer producer = new Producer(KAFKA, TOPIC);

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  LOG.info("Shutting down");
                  producer.close();
                }));

    producer.run();
  }
}
