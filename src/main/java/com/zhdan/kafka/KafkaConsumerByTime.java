package com.zhdan.kafka;


import com.zhdan.kafka.util.KafkaUtil;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Properties;

/**
 * kafka version: >0.10
 * 根据起止时间这个区间消费kafka消息<br>
 * step1:开始时间找到topic offset
 * step2:消费消息，根据每条消息的timestamp判断是否<=结束时间，这样就达到目的了
 * @author dongan.zhang
 * @date 2019/11/07
 **/
public class KafkaConsumerByTime {

    public static void main(String[] args) throws Exception {

        String topic = "start_log";
        String startTime = "2019-11-07 10:26:00";
        String endTime = "2019-11-07 10:28:00";
        Properties  kafkaProp = new Properties();
        kafkaProp.put("bootstrap.servers", "localhost:9092");
        kafkaProp.put("group.id", "testByTime");
        kafkaProp.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProp.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumerByTime task = new KafkaConsumerByTime(topic, kafkaProp, startTime, endTime);
        task.doTask();


    }

    private static final String PATTERN = "yyyy-MM-dd HH:mm:ss";

    /**
     * Pattern:yyyy-MM-dd HH:mm:ss
     */
    private String startTime;

    /**
     * Pattern:yyyy-MM-dd HH:mm:ss
     */
    private String endTime;

    /**
     * kafka配置
     */
    private Properties kafkaProp;

    private String topic;

    private boolean isCancel = false;


    public KafkaConsumerByTime(String topic, Properties kafkaProp, String startTime, String endTime) {
        this.topic = topic;
        this.kafkaProp = kafkaProp;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public void doTask() throws Exception{

        long sTime = DateUtils.parseDate(startTime, PATTERN).getTime();
        long eTime =  DateUtils.parseDate(endTime, PATTERN).getTime();

        Map<TopicPartition, OffsetAndTimestamp> startOffsetMap = KafkaUtil.fetchOffsetsWithTimestamp(topic, sTime, kafkaProp);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProp)) {
            consumer.assign(startOffsetMap.keySet());

            startOffsetMap.forEach((k, v) -> {
                consumer.seek(k, v.offset());
                System.out.println(topic + ":partition:" + k + ", offsets:" + v.offset());
            });
            System.out.println("开始消费");
            while(!isCancel) {
                System.out.println("循环...");
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    if (eTime == 0 || record.timestamp() <= eTime) {
                        // doSomething
                        System.out.printf("offset = %d,p = %d, timestamp = %d key = %s, value = %s \r\n", record.offset(), record.partition(), record.timestamp(), record.key(), record.value());
                    }
                }
            }
            System.out.println("结束消费");
        }catch (Exception e) {
           e.printStackTrace();
        }
        
    }

    public void setCancel(boolean cancel) {
        isCancel = cancel;
    }
}
