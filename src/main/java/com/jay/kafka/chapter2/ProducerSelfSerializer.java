package com.jay.kafka.chapter2;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author xuweijie
 */
public class ProducerSelfSerializer {

    private static final String BROKER_LIST = "localhost:9092";
    private static final String TOPIC = "topic-demo-2";

    private static Properties initConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CompanySerializer.class.getName()); // 自定义序列化器
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DemoPartitioner.class.getName()); // 分区器
        props.put(ProducerConfig.RETRIES_CONFIG, 10); // 重试
        return props;
    }


    public static void main(String[] args) {
        Properties props = initConfig();
        KafkaProducer<String, Company> producer = new KafkaProducer<>(props);

        Company company = Company.builder().name("hiddenkafka").address("China").build();

        ProducerRecord<String, Company> record = new ProducerRecord<>(TOPIC, company);
        try {
            producer.send(record).get(); // 同步
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.close();
    }
}
