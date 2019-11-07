package com.zhdan.kafka.util;


import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * @author dongan.zhang
 * @date 2019/11/07
 **/
public class KafkaUtil {

    /**
     * 根据时间戳获取kafka topic partition offset
     * @param timestamp 时间戳
     * @param properties kafka配置
     * @return Map<TopicPartition, OffsetAndTimestamp>
     */
    public static Map<TopicPartition, OffsetAndTimestamp> fetchOffsetsWithTimestamp(
            String topic, long timestamp, Properties properties) {
        Map<TopicPartition, OffsetAndTimestamp> result;

        try (KafkaConsumer<?, ?> consumer = new KafkaConsumer(properties)) {

            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);

            Map<TopicPartition, Long> partitionOffsetsRequest = new HashMap<>(partitionInfos.size());
            for (PartitionInfo partition : partitionInfos) {
                partitionOffsetsRequest.put(
                        new TopicPartition(partition.topic(), partition.partition()),
                        timestamp);
            }

            result = consumer.offsetsForTimes(partitionOffsetsRequest);
        }

        return result;
    }
}
