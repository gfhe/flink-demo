package org.apache.flink.playground.datagen;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.utility.DockerImageName;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ProducerTest {
    private final static String TOPIC = "test";
    private final static String CONSUMER_GROUP_ID = "test-consumer";

    private Producer producer;

    private Consumer consumer;

    private ExecutorService backgroundConsumerThread = Executors.newSingleThreadExecutor();

    private ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    @Before
    public void setup() {
        KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));
        kafka.start();
        String bootstrapServer = kafka.getBootstrapServers();

        //创建topic
        AdminClient adminClient = AdminClient.create(ImmutableMap.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer));
        Collection<NewTopic> topics = Collections.singletonList(new NewTopic(TOPIC, 1, (short) 1));
        adminClient.createTopics(topics);

        producer = new Producer(bootstrapServer, TOPIC);
        consumer = new Consumer(bootstrapServer, TOPIC, CONSUMER_GROUP_ID);
    }

    @Test
    public void run() {
        backgroundConsumerThread.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    consumer.consumer();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                System.out.println("[🚪Close test]");
                producer.close();
                consumer.close();
            }
        }, 3, TimeUnit.SECONDS);
        producer.run();
    }

    @After
    public void shutdown() {
        producer.close();
        consumer.close();
        backgroundConsumerThread.shutdownNow();
        scheduledExecutorService.shutdownNow();
    }
}