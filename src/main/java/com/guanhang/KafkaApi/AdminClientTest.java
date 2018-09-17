package com.guanhang.KafkaApi;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class AdminClientTest {

    private static final String TEST_TOPIC = "test-topic";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties pros = new Properties();
        //kafaka集群信息
        pros.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
        try (AdminClient client = AdminClient.create(pros)) {
            //描述集群信息
            describeCluster(client);
            createTopic(client);
            listAllTopics(client);
            describeTopics(client);
            alterConfigs(client);
            describeConfig(client);
            deleteTopics(client);
        }
    }

    private static void deleteTopics(AdminClient client) throws ExecutionException, InterruptedException {
        KafkaFuture<Void> futures = client.deleteTopics(Arrays.asList(TEST_TOPIC)).all();
        futures.get();
    }

    private static void describeConfig(AdminClient client) throws ExecutionException, InterruptedException {
        DescribeConfigsResult ret = client.describeConfigs(Collections.singleton(new ConfigResource(ConfigResource.Type.TOPIC, TEST_TOPIC)));
        Map<ConfigResource, Config> configs = ret.all().get();
        for (Map.Entry<ConfigResource, Config> entry : configs.entrySet()) {
            ConfigResource key = entry.getKey();
            Config value = entry.getValue();
            System.out.println(String.format("Resource type:%s,resource name:%s", key.type(), key.name()));
            Collection<ConfigEntry> configEntries = value.entries();
            for (ConfigEntry each : configEntries) {
                System.out.println(each.name() + " = " + each.value());
            }
        }
    }

    private static void alterConfigs(AdminClient client) throws ExecutionException, InterruptedException {
        Config topicConfigs = new Config(Arrays.asList(new ConfigEntry("cleanup.policy", "compact")));
        client.alterConfigs(Collections.singletonMap(new ConfigResource(ConfigResource.Type.TOPIC, TEST_TOPIC), topicConfigs)).all().get();
    }

    private static void describeTopics(AdminClient client) throws ExecutionException, InterruptedException {
        DescribeTopicsResult ret = client.describeTopics(Arrays.asList(TEST_TOPIC, "__consumer_offsets"));
        Map<String, TopicDescription> topics = ret.all().get();
        for (Map.Entry<String, TopicDescription> entry : topics.entrySet()) {
            System.out.println(entry.getKey() + "=" + entry.getValue());
        }
    }

    private static void listAllTopics(AdminClient client) throws ExecutionException, InterruptedException {
        ListTopicsOptions options = new ListTopicsOptions();
        //包括内部topics，比如__consumer_offsets
        options.listInternal(true);
        ListTopicsResult topics = client.listTopics(options);
        Set<String> topicNames = topics.names().get();
        System.out.println("Current topics in this cluster: " + topicNames);
    }

    private static void createTopic(AdminClient client) throws ExecutionException, InterruptedException {
        NewTopic newTopic = new NewTopic(TEST_TOPIC, 3, (short) 3);
        CreateTopicsResult ret = client.createTopics(Arrays.asList(newTopic));
        ret.all().get();
    }

    private static void describeCluster(AdminClient client) throws ExecutionException, InterruptedException {
        DescribeClusterResult ret = client.describeCluster();
        System.out.println(String.format("Cluster id: %s, controller: %s", ret.clusterId().get(), ret.controller().get()));
        System.out.println("Current cluster nodes info: ");
        for (Node node : ret.nodes().get()) {
            System.out.println(node);
        }
    }
}
