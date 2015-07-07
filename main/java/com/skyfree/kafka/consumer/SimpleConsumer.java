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

        // group.id: 消费组名称,要保证唯一性
        // consumer.id: 用于识别consumer,如果没有指定,kafka会自动指定
        // zookeeper.connect: 连接的zookeeper地址
        // client.id: 客户端id,同group.id
        // zookeeper.session.timeout.ms: zookeeper会话超时时间,默认6000ms
        // zookeeper.connection.timeout.ms: 建立连接的最大超时时间,默认6000ms
        // zookeeper.sync.time.ms: zookeeper leader与follower同步的最大时间,默认2000ms
        // auto.commit.enable: 启用周期性提交消息的offset给zookeeper,默认为true
        // auto.commit.interval.ms: 多久向zookeeper提交一个offset, 默认60 * 1000 ms
        // auto.offset.reset: 
        //        largest: 最大的offset (默认值)
        //        smallest:最小的offset
        //        anything: 抛出异常
        // consumer.timeout.ms: consumer等待多少毫秒后,如果没有收到消息,抛出异常,默认为-1

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
