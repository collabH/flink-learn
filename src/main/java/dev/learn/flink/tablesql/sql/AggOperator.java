package dev.learn.flink.tablesql.sql;

import dev.learn.flink.tablesql.table.StreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @fileName: SelectOperator.java
 * @description: select operator
 * @author: by echo huang
 * @date: 2020/9/6 12:01 上午
 */
public class AggOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        StreamTableEnvironment tableEnv = StreamEnvironment.getEnv(env);
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


        //group by
        Table groupTable = tableEnv.sqlQuery("select id,count(name) from kafkaTable group by id");

        //window group by
        Table windowGroupTable = tableEnv.sqlQuery("select id ,count(name),tumble_start(ts,interval '10' second) as start1,tumble_end(ts,interval '10' second) as end1 from kafkaTable" +
                " group by tumble(ts,interval '10' second),id");

        //over window aggregation 前n+1行窗口
        Table overWindowTable = tableEnv.sqlQuery("select count(name) over w from kafkaTable" +
                " window w as(partition by id order by ts asc rows between 5 preceding and current row)");

        // distinct
        Table distinctTable = tableEnv.sqlQuery("select distinct id from kafkaTable");

        // group sets
        Table setsTable = tableEnv.sqlQuery("select count(name) from kafkaTable group by grouping sets((id),(name))");

        // having
        Table havingTable = tableEnv.sqlQuery("select id,count(name) from kafkaTable group by id having count(name)>2");
        

        tableEnv.toRetractStream(havingTable, Row.class)
                .print();

        env.execute();
    }
}
