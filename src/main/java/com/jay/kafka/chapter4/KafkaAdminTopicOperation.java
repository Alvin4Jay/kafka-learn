package com.jay.kafka.chapter4;

import org.apache.kafka.clients.admin.*;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * KafkaAdminClient Topic管理
 *
 * @author xuweijie
 */
public class KafkaAdminTopicOperation {

    public static void main(String[] args) {
        createTopic();
//        describeTopic();
//        deleteTopic();
    }

    /**
     * 创建topic
     */
    private static void createTopic() {
        String brokerList = "localhost:9092";
        String topic = "topic-admin";

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        AdminClient adminClient = AdminClient.create(props);

        NewTopic newTopic = new NewTopic(topic, 4, (short) 1);

        // 设置 配置的覆盖
//        Map<String, String> configs = new HashMap<>();
//        configs.put("cleanup.policy", "compact");
//        newTopic.configs(configs);

        // 自定义分区副本分配方案
//        Map<Integer, List<Integer>> replicasAssignment = new HashMap<>();
//        replicasAssignment.put(0, Arrays.asList(1));
//        replicasAssignment.put(1, Arrays.asList(1));
//        replicasAssignment.put(2, Arrays.asList(1));
//        replicasAssignment.put(3, Arrays.asList(1));
//
//        NewTopic newTopic = new NewTopic(topic, replicasAssignment);

        CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(newTopic));

        try {
            createTopicsResult.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        adminClient.close();
    }

    /**
     * 获取topic的详情
     */
    private static void describeTopic() {
        String brokerList = "localhost:9092";
        String topic = "topic-create";

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        AdminClient client = AdminClient.create(props);

        DescribeTopicsResult result = client.describeTopics(Collections.singleton(topic));
        try {
            Map<String, TopicDescription> descriptionMap = result.all().get();
            System.out.println(descriptionMap.get(topic));
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        client.close();
    }

    /**
     * 删除topic
     */
    private static void deleteTopic() {
        String brokerList = "localhost:9092";
        String topic = "topic-admin";

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        AdminClient client = AdminClient.create(props);

        try {
            client.deleteTopics(Collections.singleton(topic)).all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        client.close();
    }

}
