package com.jay.kafka.chapter2;

import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Company序列化器
 * @author xuweijie
 */
public class CompanySerializer implements Serializer<Company> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Company company) {
        if (company == null) {
            return null;
        }

        byte[] name, address;
        try {
            if (company.getName() != null) {
                name = company.getName().getBytes(StandardCharsets.UTF_8);
            } else {
                name = new byte[0];
            }
            if (company.getAddress() != null) {
                address = company.getAddress().getBytes(StandardCharsets.UTF_8);
            } else {
                address = new byte[0];
            }

            ByteBuffer byteBuffer = ByteBuffer.allocate(4 + 4 + name.length + address.length);
            byteBuffer.putInt(name.length);
            byteBuffer.put(name);
            byteBuffer.putInt(address.length);
            byteBuffer.put(address);
            return byteBuffer.array();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

    @Override
    public void close() {

    }
}
