package dev.learn.flink.tablesql.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.CURRENT_RANGE;
import static org.apache.flink.table.api.Expressions.UNBOUNDED_RANGE;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * @fileName: UseKafkaConnector.java
 * @description: UseKafkaConnector.java类说明
 * @author: by echo huang
 * @date: 2020/9/3 10:59 下午
 */
public class UseAggOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        StreamTableEnvironment tableEnv = StreamEnvironment.getEnv(env);

//        tableEnv.getConfig().getConfiguration().set(CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
//        tableEnv.getConfig().getConfiguration().set(CHECKPOINTING_INTERVAL, Duration.ofSeconds(30));

        tableEnv.executeSql("CREATE TABLE kafkaTable (\n" +
                " id BIGINT,\n" +
                " name VARCHAR(32),\n" +
                " seq BIGINT,\n" +
                " user_time AS PROCTIME()\n" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'flink_kafka_topic',\n" +
                " 'properties.bootstrap.servers' = 'hadoop:9092,hadoop:9093,hadoop:9094',\n" +
                " 'properties.group.id' = 'testGroup',\n" +
                " 'format' = 'csv'\n" +
                ")");


        Table kafkaTable = tableEnv.from("kafkaTable");

        // groupBy window
        Table groupByWindow = kafkaTable.window(Tumble.over(lit(20).second()).on($("user_time")).as("w"))
                .groupBy($("id"), $("w"))
                .select($("id"), $("w").start().as("start_time"),
                        $("w").end().as("end_time"),
                        $("name").count().as("count1"));

        // over window
        Table overWindow = kafkaTable.window(Over.partitionBy($("id"))
                .orderBy($("user_time")).preceding(UNBOUNDED_RANGE)
                .following(CURRENT_RANGE)
                .as("w"))
                .select($("id").max().over($("w")));

        //  group by distinct
        Table distinctGroupBy = kafkaTable
                .window(Tumble.over(lit(20).second()).on($("user_time")).as("w"))
                .groupBy($("id"), $("name"), $("w"))
                .select($("id"), $("seq").count().distinct());

        // distinct
        Table distinct = kafkaTable.distinct();


        tableEnv.toRetractStream(distinct, Row.class).print();
        env.execute();
    }
}
