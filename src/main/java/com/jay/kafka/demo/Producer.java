package com.jay.kafka.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author xuweijie
 */
public class Producer {

    private static final String BROKER_LIST = "localhost:9092";
    private static final String TOPIC = "topic-demo-2";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("bootstrap.servers", BROKER_LIST);

        // 配置生产者客户端参数并创建 KafkaProducer 实例
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 构建所需要发送的消息
        ProducerRecord<String, String> message = new ProducerRecord<>(TOPIC, "hi, kafka!");

        try {
            // 发送消息
            producer.send(message);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 关闭生产者客户端实例
        producer.close();
    }

}
