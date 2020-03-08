package com.zhdan.hbase;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import sun.awt.SunHints;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author zhdan
 * @date 2020-03-07
 */
public class HBaseDemo {

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        Properties prop = new Properties();
        prop.put("bootstrap.servers", "localhost:9092");
        prop.put("group.id", "flink-streaming-job");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("start_log", new SimpleStringSchema(), prop);
        //source
        DataStream<String> source = env.addSource(consumer);
        DataStream<String> filterDs =  source.filter((FilterFunction<String>) value -> value != null && value.length() > 0);
        /*
        filterDs.map((MapFunction<String, List<Put>>) value -> {
                    List<Put> list = new ArrayList<>();
                    String[] args1 = value.split(",");

                    String rowKey = args1[0] + "_" + args1[1];
                    Put put = new Put(Bytes.toBytes(rowKey));
                    put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("column0"),Bytes.toBytes(args1[2]));
                    put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("column1"),Bytes.toBytes(args1[3]));
                    list.add(put);
                    return list;
                })
                .addSink(new HBaseSink());
        */

        //HBaseSink中入参是List<Put>，但是上面的实现只传一条数据，明显效率很低
        //用下面的方法批量Sink到HBase
        filterDs.countWindowAll(100)
                .apply((AllWindowFunction<String, List<Put>, GlobalWindow>) (window, message, out) -> {
                    List<Put> putList1 = new ArrayList<>();
                    for (String value : message) {
                        String[] columns = value.split(",");
                        String rowKey = columns[0] + "_" + columns[1];
                        Put put = new Put(Bytes.toBytes(rowKey));
                        for (int i = 2; i < columns.length; i++) {
                            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("column" + (i-2)), Bytes.toBytes(columns[i]));
                        }
                        putList1.add(put);
                    }
                     out.collect(putList1);
                 })
                .addSink(new HBaseSink());
    }
}
