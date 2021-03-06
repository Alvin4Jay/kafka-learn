package com.jay.kafka.chapter2;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author xuweijie
 */
public class KafkaProducerAnalysis {

    private static final String BROKER_LIST = "localhost:9092";
    private static final String TOPIC = "topic-demo-2";

    private static Properties initConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");
        props.put(ProducerConfig.RETRIES_CONFIG, 10); // 重试
        return props;
    }


    public static void main(String[] args) {
        Properties props = initConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "hello, Kafka!");
        try {
            producer.send(record).get(); // 同步
            // producer.send(record, new Callback() {
            //     @Override
            //     public void onCompletion(RecordMetadata metadata, Exception exception) {
            //         if (exception != null) {
            //             exception.printStackTrace();
            //         } else {
            //             System.out.println(metadata.topic() + "-" + metadata.partition() + ":" + metadata.offset());
            //         }
            //     }
            // });
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.close();
    }
}
