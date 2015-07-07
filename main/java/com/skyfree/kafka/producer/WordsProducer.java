package com.skyfree.kafka.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/4/15 下午12:02
 */
public class WordsProducer {
    private static String METAMORPHOSIS_OPENING_PARA = "One morning, when  Gregor Samsa woke from troubled dreams, " 
            + "he found himself transformed in his bed into a horrible " 
            + "vermin. He lay on his armour-like back, and if he lifted " 
            + "his head a little he could see his brown belly, slightly " 
            + "domed and divided by arches into stiff sections.";

    public static void main(String[] args) {
        Properties props = new Properties();

        /**
         * 需要说明的几点问题
         * 1. 必须在运行java的机器上能够ping通broker,比如skyfree3, skyfree5, skyfree6的区别
         * 2. kafka配置中的host.name必须设定为对应的主机名，比如host.name=l-skyfree.ops.dev.cn0.qunar.com
         * 3. kafka配置中的zookeeper中的主机，也要再运行java的机器上联通，所以配置如下
         *    zookeeper.connect=skyfree:2181,skyfree1:2181,skyfree2:2181
         */
        props.put("metadata.broker.list", "skyfree3:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);

        
        for (String word : METAMORPHOSIS_OPENING_PARA.split("\\s")) {
            System.out.println(word);
            KeyedMessage<String, String> msg = new KeyedMessage<String, String>("words_topic", word);
            producer.send(msg);
        }

        System.out.println("Produced data");
        
        producer.close();

    }
}
