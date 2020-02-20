package com.jay.kafka.chapter4;

import kafka.admin.TopicCommand;

/**
 * TopicCommand示例
 *
 * @author xuweijie
 */
public class TopicCommandUtil {

    public static void main(String[] args) {
        // createTopic();
        // describeTopic();
        listTopics();
    }

    /**
     * 创建主题
     */
    private static void createTopic() {
        String[] options = new String[]{
                "--zookeeper", "localhost:2181/kafka",
                "--create",
                "--topic", "topic-create-api",
                "--partitions", "1",
                "--replication-factor", "1"
        };
        TopicCommand.main(options);
    }

    /**
     * 查看主题
     */
    private static void describeTopic() {
        String[] options = new String[]{
                "--zookeeper", "localhost:2181/kafka",
                "--describe",
                "--topic", "topic-create"
        };
        TopicCommand.main(options);
    }

    /**
     * 列举主题
     */
    private static void listTopics() {
        String[] options = new String[]{
                "--zookeeper", "localhost:2181/kafka",
                "--list"
        };
        TopicCommand.main(options);
    }


}
