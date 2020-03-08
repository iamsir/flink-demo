/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zhdan.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingTableJob {

	public static void main(String[] args) throws Exception {

	    EnvironmentSettings settings = EnvironmentSettings
				.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //source
        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("start_log")
                .property("bootstrap.servers", "localhost:9092")
                .property("zookeeper.connect", "localhost:2181")
                //.startFromSpecificOffset()
        ).withFormat(new Json()
                .failOnMissingField(true)
                .deriveSchema()
        ).withSchema(new Schema()
                .field("mid_id", Types.STRING)
                .field("user_id", Types.INT)
                .field("version_code", Types.STRING)
                .field("version_name", Types.STRING)
                .field("lang", Types.STRING)
                .field("source", Types.STRING)
                .field("os", Types.STRING)
                .field("area", Types.STRING)
                .field("model", Types.STRING)
                .field("brand", Types.STRING)
                .field("sdk_version", Types.STRING)
                .field("gmail", Types.STRING)
                .field("height_width", Types.STRING)
                .field("app_time", Types.SQL_TIMESTAMP)
                .field("network", Types.STRING)
                .field("lng", Types.FLOAT)
                .field("lat", Types.FLOAT))
        .inAppendMode()
        .registerTableSource("start_log_source");


//        Table result = tableEnv.sqlQuery("select * from start_log_source");
//        tableEnv.toAppendStream(result, Row.class).print();

        // sink
        String sinkSql = "CREATE TABLE start_log_sink ( " +
                "    mid_id VARCHAR, " +
                "    user_id INT, " +
                "    event_time_test TIMESTAMP " +
                ") WITH ( " +
                "    'connector.type' = 'jdbc', " +
                "    'connector.url' = 'jdbc:mysql://localhost:3306/flink_test', " +
                "    'connector.table' = 'start_log_to_mysql', " +
                "    'connector.username' = 'root', " +
                "    'connector.password' = 'Aa123456', " +
                "    'connector.write.flush.max-rows' = '1' " +
                ")";

        tableEnv.sqlUpdate(sinkSql);


        String insertSql =
                "insert into start_log_sink " +
                        "select mid_id, user_id, app_time " +
                        "from start_log_source";

        tableEnv.sqlUpdate(insertSql);

        // execute program
		env.execute("Flink Streaming Java Table API Skeleton");
	}
}
