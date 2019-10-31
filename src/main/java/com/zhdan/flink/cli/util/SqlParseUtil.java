package com.zhdan.flink.cli.util;

import org.apache.calcite.sql.SqlNode;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.calcite.FlinkPlannerImpl;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.KafkaValidator;
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

import java.util.*;

import static org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_PROPERTIES;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_PROPERTIES_KEY;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_PROPERTIES_VALUE;

/**
 *
 * @author dongan.zhang
 * @date 2019/10/28
 **/
public class SqlParseUtil {


    private static final TableConfig tableConfig = new TableConfig();
    private static final Catalog catalog = new GenericInMemoryCatalog("MockCatalog", "default");
    private static final CatalogManager catalogManager = new CatalogManager("builtin", catalog);
    private static final FunctionCatalog functionCatalog = new FunctionCatalog(catalogManager);
    private static final PlanningConfigurationBuilder planningConfigurationBuilder =
            new PlanningConfigurationBuilder(tableConfig, functionCatalog,
                    asRootSchema(new CatalogManagerCalciteSchema(catalogManager, false)),
                    new ExpressionBridge<>(functionCatalog, PlannerExpressionConverter.INSTANCE()));



    /**
     * 将startup-mode = specific-offsets AND 'connector.timestamp' != (null OR '') 的sql转为startup-mode = specific-offsets模式的标准sql.
     * 不符合的返回原sql
     * @return
     */
    public static String convertToSpecificOffsetSql(String sql) {
        final FlinkPlannerImpl planner =  getPlannerBySqlDialect(SqlDialect.DEFAULT);
        SqlNode sqlNode = planner.parse(sql);
        if(sqlNode instanceof SqlCreateTable) {
            Operation operation = SqlToOperationConverter.convert(planner, sqlNode);
            if(operation instanceof CreateTableOperation) {
                CreateTableOperation op = (CreateTableOperation) operation;
                CatalogTable catalogTable = op.getCatalogTable();
                return convert(catalogTable, sql);
            }
        }

        // 如果不符合，return 原sql
        return sql;
    }


    private static String convert(CatalogTable catalogTable, String sql) {
        String startupMode =  catalogTable.getProperties().get(KafkaValidator.CONNECTOR_STARTUP_MODE);
        if(KafkaValidator.CONNECTOR_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS.equalsIgnoreCase(startupMode)) {
            long tmstamp = -1;
            try {
                tmstamp = Long.parseLong(catalogTable.getProperties().get("connector.timestamp"));
            } catch (Exception e){

            }
            //说明根据时间戳消费
            if(tmstamp > 0) {
                DescriptorProperties descriptorProperties = getValidatedProperties(catalogTable.getProperties());
                Properties kafkaProperties = getKafkaProperties(descriptorProperties);
                kafkaProperties.put(KafkaValidator.CONNECTOR_TOPIC, catalogTable.getProperties().get(KafkaValidator.CONNECTOR_TOPIC));
                kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                
                Map<KafkaTopicPartition, Long> partitionLongMap = fetchOffsetsWithTimestamp(tmstamp, kafkaProperties);
                int partitionIndex = 0;
                StringBuilder OffsetConfig = new StringBuilder();
                for (Map.Entry<KafkaTopicPartition, Long> entry : partitionLongMap.entrySet()) {
                    String partition = "'connector.specific-offsets." + partitionIndex + ".partition' = '" + entry.getKey().getPartition() + "'";
                    String offset = "'connector.specific-offsets." + partitionIndex + ".offset' = '" + entry.getValue() + "'";
                    OffsetConfig.append(", " + partition + ", " + offset);
                    partitionIndex++;
                }
                StringBuilder changeSql = new StringBuilder(sql);
                //假设WITH()代码放在最后，那么可以用如下方法将specific-offsets信息插入原始sql中
                int offset = changeSql.lastIndexOf(")");
                String newSql = changeSql.insert(offset, OffsetConfig).toString();
                //去掉connector.timestamp
                newSql = newSql.replaceAll("'connector.timestamp'(.*?),", "");
                return newSql;
            }
        }
        // 不符合回返原sql
        return sql;
    }


    // ****************** 以下把flink中的实现拿出来改一下 ****************

    private static Map<KafkaTopicPartition, Long> fetchOffsetsWithTimestamp(
            long timestamp, Properties properties) {

        String topic = properties.getProperty(KafkaValidator.CONNECTOR_TOPIC);

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


    private static FlinkPlannerImpl getPlannerBySqlDialect(SqlDialect sqlDialect) {
        tableConfig.setSqlDialect(sqlDialect);
        return planningConfigurationBuilder.createFlinkPlanner(catalogManager.getCurrentCatalog(),
                catalogManager.getCurrentDatabase());
    }

    private static DescriptorProperties getValidatedProperties(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);

        // allow Kafka timestamps to be used, watermarks can not be received from source
        //new SchemaValidator(true, true, false).validate(descriptorProperties);
        //new KafkaValidator().validate(descriptorProperties);

        return descriptorProperties;
    }

    private static Properties getKafkaProperties(DescriptorProperties descriptorProperties) {
        final Properties kafkaProperties = new Properties();
        final List<Map<String, String>> propsList = descriptorProperties.getFixedIndexedProperties(
                CONNECTOR_PROPERTIES,
                Arrays.asList(CONNECTOR_PROPERTIES_KEY, CONNECTOR_PROPERTIES_VALUE));
        propsList.forEach(kv -> kafkaProperties.put(
                descriptorProperties.getString(kv.get(CONNECTOR_PROPERTIES_KEY)),
                descriptorProperties.getString(kv.get(CONNECTOR_PROPERTIES_VALUE))
        ));
        return kafkaProperties;
    }



}
