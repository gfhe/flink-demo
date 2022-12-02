package org.apache.flink.playground.datagen;

import java.util.Arrays;
import java.util.Properties;
import org.apache.flink.playground.datagen.model.StructuredData;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Consumer {
  private Logger log = LoggerFactory.getLogger(Consumer.class);

  private volatile boolean isRunning;

  private final String brokers;

  private final String topic;

  private final String groupId;

  public Consumer(String brokers, String topic, String groupId) {
    this.brokers = brokers;
    this.topic = topic;
    this.groupId = groupId;
    this.isRunning = true;
  }

  public void consumer() throws InterruptedException {
    KafkaConsumer<String, StructuredData> consumer =
        new KafkaConsumer<String, StructuredData>(getProperties());
    consumer.subscribe(Arrays.asList(topic));

    while (isRunning) {
      ConsumerRecords<String, StructuredData> records = consumer.poll(1000);

      // 遍历处理数据
      for (ConsumerRecord<String, StructuredData> record : records) {
        System.out.println("[💰consumer]: key=" + record.key() + ", value=" + record.value());
      }
    }
    // 关闭消费者
    consumer.close();
  }

  public void close() {
    this.isRunning = false;
  }

  private Properties getProperties() {
    final Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StructureDataDeserializer.class);
    return props;
  }
}
