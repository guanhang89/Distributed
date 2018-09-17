package com.guanhang;

import java.util.ArrayList;
import java.util.List;

public class ConsumerGroup {

    private List<ConsumerRunnable> consumers;

    public ConsumerGroup(int consumerNum, String groupId, String topic, String brokerList) {
        consumers = new ArrayList<>();
        for(int i = 0;i<consumerNum;++i) {
            ConsumerRunnable consumerRunnable = new ConsumerRunnable(brokerList, groupId, topic);
            consumers.add(consumerRunnable);
        }
    }

    public void execute(){
        for (ConsumerRunnable task : consumers) {
            new Thread(task).start();
        }
    }

    public static void main(String[] args) {
        String brokerList = "localhost:9092";
        String groupId = "testGroup";
        String topic = "test-topic";
        int consumerNum = 3;

        ConsumerGroup consumerGroup = new ConsumerGroup(consumerNum, groupId, topic, brokerList);
        consumerGroup.execute();
    }
}
