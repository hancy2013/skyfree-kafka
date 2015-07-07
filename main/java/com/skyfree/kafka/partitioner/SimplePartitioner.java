package com.skyfree.kafka.partitioner;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/7 09:41
 */
public class SimplePartitioner implements Partitioner {
    
    // 这个构造函数必须有,但是我又没有看到在哪里有,郁闷,估计是在哪个地方,进行了反射调用
    public SimplePartitioner(VerifiableProperties props) {
    }
    
    // 这是关键,实现了由Key到分区数字的映射关系, partitionNumber是该topic用于的分区数量
    public int partition(Object key, int partitionNumber) {
        int partition = 0;
        String partitionKey = (String) key;

        int offset = partitionKey.lastIndexOf('.');

        if (offset > 0) {
            partition = Integer.parseInt(partitionKey.substring(offset + 1)) % partitionNumber;
        }

        return partition;
    }
}
