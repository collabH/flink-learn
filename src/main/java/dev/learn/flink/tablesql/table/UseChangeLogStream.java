package dev.learn.flink.tablesql.table;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @fileName: UseChanageLogStream.java
 * @description: UseChanageLogStream.java类说明
 * @author: by echo huang
 * @date: 2020/9/5 12:34 下午
 */
public class UseChangeLogStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamEnvironment.getEnv(env);

        DataStreamSource<String> datasource = env.socketTextStream("hadoop", 10000);


        Table table = tableEnv.fromDataStream(datasource, $("name"));

        // filesystem/kafka不支持
        Table aggTable = table.groupBy($("name"))
                .select($("name"), $("name").count().as("count"));


        tableEnv.executeSql("CREATE TABLE kafkaTable (\n" +
                " name STRING\n" +
//                " `count` BIGINT\n" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'flink_kafka_topic',\n" +
                " 'properties.bootstrap.servers' = 'hadoop:9092,hadoop:9093,hadoop:9094',\n" +
                " 'format' = 'csv'\n" +
                ")");

        table.executeInsert("kafkaTable");

        // retract mode
//        tableEnv.toRetractStream(aggTable, Row.class);
        // append mode
//        tableEnv.toAppendStream(table, Row.class);

        // upsert mode

//        env.execute();

    }
}
