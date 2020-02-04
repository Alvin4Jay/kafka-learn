package com.jay.kafka.chapter3;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 消费位移的演示
 *
 * @author xuweijie
 */
@Slf4j
public class CheckOffsetAndPosition {

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
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }

    public static void main(String[] args) {
        Properties properties = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        TopicPartition tp = new TopicPartition(TOPIC, 0);
        consumer.assign(Arrays.asList(tp));

        try {
            long lastConsumedOffset = -1; // 当前消费到的位移
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if (records.isEmpty()) {
                    break;
                }
                List<ConsumerRecord<String, String>> partitionRecords = records.records(tp);
                lastConsumedOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                consumer.commitSync(); // 同步提交消费位移
            }
            System.out.println("last consumed offset is " + lastConsumedOffset);
            OffsetAndMetadata offsetAndMetadata = consumer.committed(tp);
            System.out.println("commited offset is " + offsetAndMetadata.offset());
            long position = consumer.position(tp);
            System.out.println("the offset of the next record is " + position);
        } catch (Exception e) {
            log.error("occur exception", e);
        } finally {
            consumer.close();
        }
    }

}
