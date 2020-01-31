package com.jay.kafka.chapter2;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 自定义生产者拦截器
 *
 * @author xuweijie
 */
public class ProducerInterceptorPrefix implements ProducerInterceptor<String, String> {

    private volatile long sendSuccess = 0;
    private volatile long sendFail = 0;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
       String modifiedValue = "prefix1-" + record.value();
       return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(),
               record.key(), modifiedValue, record.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            sendSuccess++;
        } else {
            sendFail++;
        }
    }

    @Override
    public void close() {
        double successRatio = (double) sendSuccess / (sendFail + sendSuccess);
        System.out.println("[INFO] 发送成功率="
                + String.format("%f", successRatio * 100) + "%");
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
