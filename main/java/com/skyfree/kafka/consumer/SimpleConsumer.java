package com.skyfree.kafka.consumer;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;


import java.io.Closeable;
import java.io.IOException;
import java.util.*;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/7 12:09
 */
public class SimpleConsumer implements Closeable {
    private ConsumerConnector consumer;
    private String topic;

    private ConsumerConfig createConsumerConfig(String zookeeper, String groupId) {
        Properties props = new Properties();

        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "500");
        props.put("zookeeper.sync.time.ms", "250");
        props.put("auto.commit.interval.ms", "1000");

        return new ConsumerConfig(props);
    }

    public SimpleConsumer(String zookeeper, String groupId, String topic) {
        ConsumerConfig config = createConsumerConfig(zookeeper, groupId);
        consumer = Consumer.createJavaConsumerConnector(config);
        this.topic = topic;
    }

    public void processMessages() {
        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        topicMap.put(this.topic, 1);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreamMap = consumer.createMessageStreams(topicMap);
        List<KafkaStream<byte[], byte[]>> streamList = consumerStreamMap.get(this.topic);
        for (KafkaStream<byte[], byte[]> stream : streamList) {
            for (MessageAndMetadata<byte[], byte[]> aStream : stream) {
                String message = new String(aStream.message());
                // 这里就会持续获取消息了,这里只是简单的输出了消息,可以在这里直接处理消息,也可以输入一个回调函数到这里
                System.out.println(message);
            }
        }

    }

    public void close() throws IOException {
        if (consumer != null) {
            consumer.shutdown();
        }
    }

    public static void main(String[] args) throws IOException {
        SimpleConsumer consumer = new SimpleConsumer("skyfree1:2181", "skyfree-group", "skyfree_test2");
        consumer.processMessages();
        consumer.close();
    }
}
