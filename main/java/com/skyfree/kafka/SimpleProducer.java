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
        
        // 所有属性的描述
        // metadata.broker.list: broker列表,格式如例子所示
        // serializer.class: 执行序列化的类,可以自定义该类,默认:kafka.serializer.DefaultEncoder
        // producer.typ: async或sync, 指定为异步方式或同步方式,默认为sync
        // request.required.acks:   0: producer不等待ack
        //                          1: 主副本接受消息,就返回ack
        //                          -1: 所有副本同步完成,才返回ack
        // key.serializer.class: key的序列化器,默认同serializer.class
        // partitioner.class: 分区器,参看本例子com.skyfree.kafka.partitioner.SimplePartitioner,
        //                        默认为:kafka.producer.DefaultPartitioner
        // compression.codec: none,gzip, snappy 指定消息的压缩方式,默认为none
        // batch.num.messages: 如果使用异步方式,producer收集到这么多消息后,才会发送出去
        // queue.buffer.ms: 如果使用异步方式,producer等待时间超过这么多秒,才会发送消息,与batch.num.messages是满足谁都可以的关系.
        
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
