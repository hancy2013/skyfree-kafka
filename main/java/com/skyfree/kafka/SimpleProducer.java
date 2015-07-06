package com.skyfree.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/6 18:40
 */
public class SimpleProducer implements Closeable {
    // key的数据类型为String, message的数据类型为String
    private static Producer<String, String> producer;


    public SimpleProducer() {
        String brokers = "skyfree3:9092,skyfree5:9092,skyfree6:9092";
        Properties props = new Properties();

        // broker list
        props.put("metadata.broker.list", brokers);

        // 执行序列化的class,默认的序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        // 1: lead成功写入消息后,需要返回ack
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);
    }

    public void publishMessage(String topic, String message) {
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, message);
        producer.send(data);
    }

    public void close() throws IOException {
        if (producer != null) {
            producer.close();
        }
    }

    public static void main(String[] args) throws IOException {
        SimpleProducer producer = new SimpleProducer();
        producer.publishMessage("skyfree_test", "beibei");
        producer.close();
    }

}
