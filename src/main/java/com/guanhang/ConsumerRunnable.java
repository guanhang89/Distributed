package com.guanhang;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class ConsumerRunnable implements Runnable {

    private final KafkaConsumer<String, String> consumer;

    public ConsumerRunnable(String brokerList, String groupId, String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key,deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        this.consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
    }

    @Override
    public void run() {

        while (true) {
            ConsumerRecords<String, String> poll = consumer.poll(200);
            for (ConsumerRecord record : poll) {
                System.out.println(Thread.currentThread().getName() + "consumed " + record.partition() +
                        "th message with offset: " + record.offset());
            }
        }
    }
}
