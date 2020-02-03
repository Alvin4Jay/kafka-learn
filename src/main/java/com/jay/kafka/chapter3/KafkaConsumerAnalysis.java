package com.jay.kafka.chapter3;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author xuweijie
 */
@Slf4j
public class KafkaConsumerAnalysis {

    private static final String BROKER_LIST = "localhost:9092";
    private static final String TOPIC = "topic-demo-2";
    private static final String GROUP = "group.demo";
    private static AtomicBoolean isRunning = new AtomicBoolean(true);

    private static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer.client.id.demo");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return properties;
    }

    public static void main(String[] args) {
        Properties properties = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // consumer.subscribe(Collections.singleton(TOPIC)); // 订阅特定topic
        consumer.assign(Arrays.asList(new TopicPartition(TOPIC, 0))); // 订阅特定topic的特定分区

        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                //for (ConsumerRecord<String, String> record : records) {
                //    System.out.println("topic=" + record.topic() + ", partition=" + record.partition()
                //            + ", offset=" + record.offset() + ",");
                //    System.out.println("key=" + record.key() + ", value=" + record.value());
                //    // do something to process record.
                //}
                for (TopicPartition partition : records.partitions()) {
                    for (ConsumerRecord<String, String> record : records.records(partition)) {// 按照分区获取消息
                        System.out.println(record.partition() + " : " + record.value());
                    }
                }
            }
        } catch (Exception e) {
            log.error("occur exception", e);
        } finally {
            consumer.close();
        }
    }

}
