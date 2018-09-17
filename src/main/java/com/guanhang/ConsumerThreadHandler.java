package com.guanhang;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ConsumerThreadHandler<K, V> {
    private final KafkaConsumer<K, V> consumer;
    private ExecutorService executors;
    private final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

    public ConsumerThreadHandler(String brokerList, String groupId, String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("key,deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                //提交位移
                consumer.commitSync(                                                );
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                offsets.clear();
            }
        });
    }

    public void consumer(int threadNum) {
        executors = new ThreadPoolExecutor(threadNum,
                threadNum,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(1000),
                new ThreadPoolExecutor.CallerRunsPolicy());
        try {
            while (true) {
                ConsumerRecords<K, V> records = consumer.poll(1000);
                if (!records.isEmpty()) {
                    executors.submit(new ConsumerWorker<>(records, offsets));
                }
                commitOffsets();
            }
        } catch (WakeupException e) {
            //忽略
        }finally {
            commitOffsets();
            consumer.close();
        }
    }

    private void commitOffsets() {
        Map<TopicPartition, OffsetAndMetadata> unmodifiedMap;
        synchronized (offsets) {
            if (offsets.isEmpty()) {
                return;
            }
            unmodifiedMap = Collections.unmodifiableMap(new HashMap<>(offsets));
            offsets.clear();
        }
        consumer.commitSync(unmodifiedMap);
    }

    public void close() {
        consumer.wakeup();
        executors.shutdown();
    }
}
