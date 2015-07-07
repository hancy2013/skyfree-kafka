package com.skyfree.kafka.consumer;

import kafka.consumer.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/7 14:02
 */
public class MultiThreadConsumer implements Closeable {
    private ExecutorService executor;

    private final kafka.javaapi.consumer.ConsumerConnector consumer;
    private final String topic;

    private ConsumerConfig createConsumerConfig(String zookeeper, String groupId) {
        Properties props = new Properties();

        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "500");
        props.put("zookeeper.sync.time.ms", "250");
        props.put("auto.commit.interval.ms", "1000");

        return new ConsumerConfig(props);
    }

    public MultiThreadConsumer(String zookeeper, String groupId, String topic) {
        ConsumerConfig config = createConsumerConfig(zookeeper, groupId);
        consumer = Consumer.createJavaConsumerConnector(config);
        this.topic = topic;
    }

    public void close() throws IOException {
        if (consumer != null) {
            consumer.shutdown();
        }

        if (executor != null) {
            executor.shutdown();
        }
    }

    // 这里的线程数,要与topic的分区数目保持一致
    public void processMessages(int threadCount) {
        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        topicMap.put(this.topic, threadCount);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreamMap = consumer.createMessageStreams(topicMap);

        List<KafkaStream<byte[], byte[]>> streamList = consumerStreamMap.get(this.topic);

        executor = Executors.newFixedThreadPool(threadCount);

        int count = 0;
        for (final KafkaStream<byte[], byte[]> stream : streamList) {
            final int threadNumber = count;
            executor.submit(new Runnable() {
                public void run() {
                    ConsumerIterator<byte[], byte[]> iter = stream.iterator();
                    while (iter.hasNext()) {
                        String message = new String(iter.next().message());
                        System.out.println(message + ":thread " + threadNumber);
                    }
                }
            });
            count++;
        }
    }

    public static void main(String[] args) throws IOException {
        MultiThreadConsumer consumer = new MultiThreadConsumer("skyfree1:2181", "skyfree-test1-group", "skyfree_test1");
        consumer.processMessages(4);
        try {
            Thread.sleep(100000);
        } catch (InterruptedException e) {
            consumer.close();
        }

    }
}
