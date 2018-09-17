package com.guanhang;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties pros = new Properties();
        //必须指定
        pros.put("bootstrap.servers", "localhost:9092");
        //必须指定
        pros.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //必须指定
        pros.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        pros.put("acks", "-1");
        pros.put("retries", 3);
        pros.put("batch.size", 323840);
        pros.put("linger.ms", 10);
        pros.put("buffer.memory", 33554432);
        pros.put("max.block.ms", 3000);
        KafkaProducer<Object, Object> producer = new KafkaProducer<>(pros);
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<Object, Object>("my-topic", Integer.toString(i), Integer.toString(i)));
            producer.close();
        }
        ProducerRecord<Object, Object> record = new ProducerRecord<Object, Object>("my-topic", Integer.toString(1), Integer.toString(1));
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                }else {
                }
            }
        });
        RecordMetadata recordMetadata = producer.send(record).get();
    }
}
