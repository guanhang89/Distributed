package com.guanhang;

public class MultiMain {
    public static void main(String[] args) {
        String brokerList = "localhost:9092";
        String topic = "test-topic";
        String groupId = "test-group";
        final ConsumerThreadHandler<Object, Object> handler = new ConsumerThreadHandler<>(brokerList, groupId, topic);
        final int cpuCount = Runtime.getRuntime().availableProcessors();
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                handler.consumer(cpuCount);
            }
        };
        new Thread(runnable).start();

        try {
            Thread.sleep(20000L);
        } catch (InterruptedException e) {
            //忽略
        }
        System.out.println("Starting to close the consumer....");
        handler.close();
    }
}
