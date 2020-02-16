package com.jay.kafka.demo.canal;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Kafka消费者消费Canal从MySQL binlog抽取出的消息
 *
 * @author xuweijie
 */
public class CanalKafkaConsumer {

    private static final String BROKER_LIST = "localhost:9092";
    private static final String TOPIC = "xuweijie.test";
    private static final String GROUP = "test";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("bootstrap.servers", BROKER_LIST);

        // 设置消费组的名称
        properties.put("group.id", GROUP);

        // 创建一个消费者客户端实例
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // 1.订阅主题
        consumer.subscribe(Collections.singleton(TOPIC));

        // 2.循环消费消息
        while (true) {
            ConsumerRecords<String, String> messages = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> message : messages) {
                System.out.println(message.value());
            }
        }
    }

}
