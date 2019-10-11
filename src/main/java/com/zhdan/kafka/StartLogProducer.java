package com.zhdan.kafka;

import com.cloudwise.sdg.dic.DicInitializer;
import com.cloudwise.sdg.template.TemplateAnalyzer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * @author dongan.zhang
 * @description 模拟产生app启动日志数据
 * @create 2019-08-20 09:25
 **/
@Slf4j
public class StartLogProducer {

    private static final int DATA_LEN = 10000;

    private static final String TOPIC_NAME = "start_log";

    public static void main(String[] args) throws Exception {

        //加载词典(只需执行一次即可)
        DicInitializer.init();

        //编辑模版
        String userEventTpl = "{" +
                "\"mid_id\":\"$Dic{mid_id}\"," +
                "\"user_id\":\"$Dic{user_id}\"," +
                "\"version_code\":\"$Dic{version_code}\"," +
                "\"version_name\":\"$Dic{version_name}\"," +
                "\"lang\":\"$Dic{lang}\"," +
                "\"source\":\"$Dic{source}\"," +  //渠道
                "\"os\":\"$Dic{os}\"," +
                "\"area\":\"$Dic{area}\"," +
                "\"model\":\"$Dic{model}\"," +
                "\"brand\":\"$Dic{brand}\"," +
                "\"sdk_version\":\"$Dic{sdk_version}\"," +
                "\"gmail\":\"$Dic{gmail}\"," +
                "\"height_width\":\"$Dic{height_width}\"," +
                "\"app_time\":\"$Dic{app_time}\"," +
                "\"network\":\"$Dic{network}\"," +
                "\"lng\":\"$Dic{lng}\"," +
                "\"lat\":\"$Dic{lat}\"" +
                "}";

        //创建模版分析器
        TemplateAnalyzer userEventTplAnalyzer = new TemplateAnalyzer("userEvent", userEventTpl);

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        ProducerRecord<String, String> record;

        //模拟产生数据并存入kafka
        for(int i = 0; i < DATA_LEN; i++){
            String data = userEventTplAnalyzer.analyse();
            System.out.println(userEventTplAnalyzer.analyse());
            record = new ProducerRecord<>(TOPIC_NAME,
                    new Random().nextInt()+"",
                    data);
            //kafkaProducer.send(record);
            
            long sleep = (long) (Math.random()*2000);
            Thread.sleep(sleep);

        }



    }
}