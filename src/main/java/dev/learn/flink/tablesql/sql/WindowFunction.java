package dev.learn.flink.tablesql.sql;

import dev.learn.flink.tablesql.table.StreamEnvironment;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_MODE;

/**
 * @fileName: WindowFunction.java
 * @description: WindowFunction.java类说明
 * @author: by echo huang
 * @date: 2020/9/5 9:03 下午
 */
public class WindowFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        StreamTableEnvironment tableEnv = StreamEnvironment.getEnv(env);
        tableEnv.getConfig().getConfiguration().set(CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
        tableEnv.getConfig().getConfiguration().set(CHECKPOINTING_INTERVAL, Duration.ofSeconds(60));

        tableEnv.executeSql("CREATE TABLE kafkaTable (\n" +
                " id BIGINT,\n" +
                " name STRING,\n" +
                " ts AS PROCTIME()" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'flink_kafka',\n" +
                " 'properties.bootstrap.servers' = 'hadoop:9092,hadoop:9093,hadoop:9094',\n" +
                " 'properties.group.id' = 'testGroup',\n" +
                " 'format' = 'csv'\n" +
                ")");

        // 滚动时间
        Table table = tableEnv.sqlQuery("select id,count(name) as num,tumble_start(ts,INTERVAL '10' second) as start_time,tumble_end(ts,INTERVAL '10' second) as end_time from kafkaTable " +
                "group by tumble(ts,INTERVAL '10' second),id");

        // topN
        Table topFive = tableEnv.sqlQuery("select id,name,row_num from(" +
                "select *,ROW_NUMBER() OVER(PARTITION BY id ORDER BY name DESC)as row_num from kafkaTable)" +
                "where row_num <=5");

        tableEnv.toRetractStream(table, Row.class)
                .print();

        env.execute();
    }
}
