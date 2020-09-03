package dev.learn.flink.tablesql.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @fileName: UseKafkaConnector.java
 * @description: UseKafkaConnector.java类说明
 * @author: by echo huang
 * @date: 2020/9/3 10:59 下午
 */
public class UseKafkaConnector {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        StreamTableEnvironment tableEnv = StreamEnvironment.getEnv(env);

        tableEnv.executeSql("CREATE TABLE kafkaTable (\n" +
                " id BIGINT,\n" +
                " name VARCHAR(32),\n" +
                " user_time AS PROCTIME()\n" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'flink_kafka_topic',\n" +
                " 'properties.bootstrap.servers' = 'hadoop:9092,hadoop:9093,hadoop:9094',\n" +
                " 'properties.group.id' = 'testGroup',\n" +
                " 'format' = 'csv',\n" +
                " 'scan.startup.mode' = 'earliest-offset'\n" +
                ")");

        Table kafkaTable = tableEnv.from("kafkaTable");

        tableEnv.toRetractStream(kafkaTable, Row.class).print();
        env.execute();
    }
}
