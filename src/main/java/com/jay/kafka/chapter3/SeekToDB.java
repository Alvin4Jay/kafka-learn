package com.jay.kafka.chapter3;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 消费位移保存到DB 3-9
 *
 * @author xuweijie
 */
public class SeekToDB {

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
        consumer.subscribe(Arrays.asList(TOPIC));

        Set<TopicPartition> assignment = new HashSet<>();
        while (assignment.size() == 0) {
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }

        for (TopicPartition topicPartition : assignment) {
            // 从DB获取消费位移
            long offset = getOffsetFromDB(topicPartition);
            consumer.seek(topicPartition, offset);
        }

        while (isRunning.get()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                for (ConsumerRecord<String, String> partitionRecord : partitionRecords) {
                    System.out.println(partitionRecord.offset() + ":" + partitionRecord.value());
                }
                long lastConsumedOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                // 将消费位移保存到DB
                storeOffsetToDB(partition, lastConsumedOffset + 1);
            }
        }
    }

    /**
     * 保存消费位移到DB
     */
    private static void storeOffsetToDB(TopicPartition partition, long position) {

    }

    /**
     * 从数据库中获取消费位移
     */
    private static long getOffsetFromDB(TopicPartition topicPartition) {
        return 0;
    }

}
