package com.zhdan.test;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.calcite.FlinkPlannerImpl;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.expressions.ExpressionBridge;
import org.apache.flink.table.expressions.PlannerExpressionConverter;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.planner.PlanningConfigurationBuilder;
import org.apache.flink.table.sqlexec.SqlToOperationConverter;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.*;

import static org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema;


/**
 * 目前问题：<br>
 *     如何从sql中提取connector.properties.0.key/connector.properties.0.value
 *     类似这种信息，然后拿到连接kafka的信息，然后封装给fetchOffsetsWithTimestamp提取
 *     topic的partition偏移量
 * @author dongan.zhang
 * @date 2019/10/26
 **/
public class SqlParseTest {

    private final TableConfig tableConfig = new TableConfig();
    private final Catalog catalog = new GenericInMemoryCatalog("MockCatalog",
            "default");
    private final CatalogManager catalogManager =
            new CatalogManager("builtin", catalog);
    private final FunctionCatalog functionCatalog = new FunctionCatalog(catalogManager);
    private final PlanningConfigurationBuilder planningConfigurationBuilder =
            new PlanningConfigurationBuilder(tableConfig,
                    functionCatalog,
                    asRootSchema(new CatalogManagerCalciteSchema(catalogManager, false)),
                    new ExpressionBridge<>(functionCatalog,
                            PlannerExpressionConverter.INSTANCE()));

    String SQL = "CREATE TABLE start_log_source( " +
            "   mid_id VARCHAR,  " +
            "   user_id INT,  " +
            "   version_code VARCHAR,  " +
            "   version_name VARCHAR,  " +
            "   lang VARCHAR,  " +
            "   source VARCHAR,  " +
            "   os VARCHAR,  " +
            "   area VARCHAR,  " +
            "   model VARCHAR,  " +
            "   brand VARCHAR,  " +
            "   sdk_version VARCHAR,  " +
            "   height_width VARCHAR,  " +
            "   app_time TIMESTAMP," +
            "   network VARCHAR,  " +
            "   lng FLOAT,  " +
            "   lat FLOAT  " +
            ") WITH ( " +
            "   'connector.type' = 'kafka',  " +
            "   'connector.version' = '0.11'," +
            "   'connector.topic' = 'start_log',  " +
            "   'connector.startup-mode' = 'specific-offsets',  " +
            "   'connector.timestamp' = '1571909309022',  " +
            "   'connector.properties.0.key' = 'zookeeper.connect',  " +
            "   'connector.properties.0.value' = 'localhost:2181',  " +
            "   'connector.properties.1.key' = 'bootstrap.servers',  " +
            "   'connector.properties.1.value' = 'localhost:9092',  " +
            "   'connector.properties.2.key' = 'group.id', " +
            "    'connector.properties.2.value' = 'testGroup', " +
            "   'update-mode' = 'append',  " +
            "   'format.type' = 'json',  " +
            "   'format.derive-schema' = 'true'  " +
            ")";

    /**
     * flink 解析 sql
     * @throws Exception
     */
    @Test
    public void parseSqlTest() throws Exception{
        final FlinkPlannerImpl planner =
                getPlannerBySqlDialect(SqlDialect.DEFAULT);
        SqlNode sqlNode = planner.parse(SQL);
        assert sqlNode instanceof SqlCreateTable;
        Operation operation = SqlToOperationConverter.convert(planner, sqlNode);
        assert operation instanceof CreateTableOperation;
        CreateTableOperation op = (CreateTableOperation) operation;
        CatalogTable catalogTable = op.getCatalogTable();


        String startupMode =  catalogTable.getProperties().get("connector.startup-mode");
        if("specific-offsets".equalsIgnoreCase(startupMode)) {
            long tmstamp = -1;
            try {
                tmstamp = Long.parseLong(catalogTable.getProperties().get("connector.timestamp"));
            } catch (Exception e){

            }
            System.out.println("timestamp:" + tmstamp);
            //说明根据时间戳消费
            if(tmstamp > 0) {
                Properties properties = new Properties();
                properties.put("topic", "start_log");
                //
                properties.put("bootstrap.servers", "localhost:9092");
                properties.put("group.id", "testGroup");
                properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

                Map<KafkaTopicPartition, Long> partitionLongMap = fetchOffsetsWithTimestamp(tmstamp, properties);

                int partitionIndex = 0;
                StringBuilder OffsetConfig = new StringBuilder();
                for (Map.Entry<KafkaTopicPartition, Long> entry : partitionLongMap.entrySet()) {
                    System.out.println("partitionToOffset *********************");

                    String partition = "'connector.specific-offsets." + partitionIndex + ".partition' = '" + entry.getKey().getPartition() + "'";
                    String offset = "'connector.specific-offsets." + partitionIndex + ".offset' = '" + entry.getValue() + "'";
                    OffsetConfig.append(", " + partition + ", " + offset);
                    
                    partitionIndex++;
                }

                StringBuilder changeSql = new StringBuilder(SQL);
                //假设WITH()代码放在最后，那么可以用如下方法将specific-offsets信息插入原始sql中
                int offset = changeSql.lastIndexOf(")");

                String newSql = changeSql.insert(offset, OffsetConfig).toString();
                System.out.println("newSql:" + newSql);
            }



        }


        //System.out.println(sqlNode);
    }

    private FlinkPlannerImpl getPlannerBySqlDialect(SqlDialect sqlDialect) {
        tableConfig.setSqlDialect(sqlDialect);
        return planningConfigurationBuilder.createFlinkPlanner(catalogManager.getCurrentCatalog(),
                catalogManager.getCurrentDatabase());
    }



    /**
     * 把flink中的实现拿出来改一下
     * @param timestamp
     * @param properties
     * @return
     */
    private Map<KafkaTopicPartition, Long> fetchOffsetsWithTimestamp(
            long timestamp, Properties properties) {

        String topic = properties.getProperty("topic");

        Map<KafkaTopicPartition, Long> result;

        try (KafkaConsumer<?, ?> consumer = new KafkaConsumer(properties)) {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            Collection<KafkaTopicPartition> partitions = new ArrayDeque<>(partitionInfos.size());
            for(PartitionInfo p : partitionInfos) {
                partitions.add(new KafkaTopicPartition(p.topic(), p.partition()));
            }

            Map<TopicPartition, Long> partitionOffsetsRequest = new HashMap<>(partitions.size());
            for (KafkaTopicPartition partition : partitions) {
                partitionOffsetsRequest.put(
                        new TopicPartition(partition.getTopic(), partition.getPartition()),
                        timestamp);
            }

            result = new HashMap<>(partitions.size());

            for (Map.Entry<TopicPartition, OffsetAndTimestamp> partitionToOffset :
                    consumer.offsetsForTimes(partitionOffsetsRequest).entrySet()) {

                result.put(
                        new KafkaTopicPartition(partitionToOffset.getKey().topic(), partitionToOffset.getKey().partition()),
                        (partitionToOffset.getValue() == null) ? null : partitionToOffset.getValue().offset());
            }
        }

        return result;
    }

}
