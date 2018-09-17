package com.guanhang;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class ConsumerDemo {
    public static void main(String[] args) {
        String topicName = "test-topic";
        String groupId = "test-group";
        Properties props = new Properties();
        props.put("bootstrap.servers", "locahost:9092");
        //必须指定
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        //从最早的消息开始读取
        props.put("auto.offset.reset", "earliest");
        //必须指定
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //必须指定
        props.put("value.seserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(props);


        List<TopicPartition> partitions = new ArrayList<>();
        List<PartitionInfo> allPartitions = consumer.partitionsFor("test-topic");
        if (allPartitions != null && !allPartitions.isEmpty()) {
            for (PartitionInfo partitionInfo : allPartitions) {
                partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
            }
            consumer.assign(partitions);
        }
/*        consumer.subscribe(Arrays.asList("test-topoic"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                //在协调器开启新一轮rebalance前会调用
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                //rebalance完成后调用
            }
        });*/
/*        consumer.subscribe(Collections.singleton(topicName));
        boolean running = true;
        try{
            while (running) {
                ConsumerRecords<Object, Object> records = consumer.poll(1000);
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<Object, Object>> partitionRecords = records.records(partition);
                    for (ConsumerRecord record : partitionRecords) {
                        System.out.println(record.offset() + ": " + record.value());
                    }
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                }
            }
        }finally {
            consumer.close();
        }*/
/*        final int minBatchSize = 500;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        while (true) {
            ConsumerRecords<Object, Object> records = consumer.poll(1000);
            for (ConsumerRecord record : records) {
                buffer.add(record);
            }
            if (buffer.size() >= minBatchSize) {
                insertIntoDb(buffer);
                consumer.commitAsync();
                buffer.clear();
            }
        }*/
/*        try {

            while (true) {
                ConsumerRecords<Object, Object> records = consumer.poll(1000);
                for (ConsumerRecord record : records) {
                    System.out.println(record.key() + ":" + record.value());
                }
            }
        }finally {
            consumer.close();
        }*/


    }

    private static void insertIntoDb(List<ConsumerRecord<String, String>> buffer) {
    }
}
