package com.skyfree.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.Closeable;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/7 09:39
 */
public class ProducerWithPartition implements Closeable {
    // key的数据类型为String, message的数据类型为String
    private static Producer<String, String> producer;


    public ProducerWithPartition() {
        String brokers = "skyfree3:9092,skyfree5:9092,skyfree6:9092";
        Properties props = new Properties();

        // broker list
        props.put("metadata.broker.list", brokers);

        // 执行序列化的class,默认的序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        // 这里自定义了分区器
        props.put("partitioner.class", "com.skyfree.kafka.partitioner.SimplePartitioner");

        // 1: lead成功写入消息后,需要返回ack
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);
    }

    public void publishMessage(String topic, String key, String message) {
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, key, message);
        producer.send(data);
    }

    public static void main(String[] args) throws IOException {
        Random random = new Random();

        ProducerWithPartition producer = new ProducerWithPartition();

        for (int i = 0; i < 100; i++) {
            String clientIP = "192.168.14." + random.nextInt(255);
            String accessTime = new Date().toString();

            String message = accessTime + ",kafka.apache.org," + clientIP;
            System.out.println(message);
            producer.publishMessage("skyfree_test", clientIP, message);
        }

        producer.close();
    }

    public void close() throws IOException {
        if (producer != null) {
            producer.close();
        }
    }
}
