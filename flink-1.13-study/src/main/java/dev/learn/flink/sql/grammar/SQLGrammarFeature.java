package dev.learn.flink.sql.grammar;

import dev.learn.flink.FlinkEnvUtils;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @fileName: SQLGrammarFeature.java
 * @description: flink sql语法
 * @author: huangshimin
 * @date: 2021/8/29 7:02 下午
 */
public class SQLGrammarFeature {
    static String expample_sql = "create table test(name string,id int,age bigint,price int,event_time " +
            "timestamp_ltz(3)," +
            "watermark for event_time as event_time - INTERVAL '1' second)with" +
            "('connector'='datagen', 'fields.id.min'='1', 'fields.id.max'='10','fields.price.min'='1', " +
            "'fields.price.max'='10','fields.age.kind'='sequence'," +
            "'fields.age.start'='1','fields.age.end'='100')";

    public static void main(String[] args) {
        StreamExecutionEnvironment streamEnv = FlinkEnvUtils.getStreamEnv();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(streamEnv);
//        with(tableEnvironment);
//        window(tableEnvironment);
        windowAgg(tableEnvironment);
    }


    /**
     * 动态参数
     *
     * @param tableEnvironment
     */
    private static void sqlHints(StreamTableEnvironment tableEnvironment) {
        tableEnvironment.getConfig().getConfiguration().set(ConfigOptions.key("table.dynamic-table-options" +
                ".enabled")
                .booleanType().defaultValue(true), true);
        tableEnvironment.executeSql("select id, name from kafka_table1 /*+ OPTIONS('scan.startup" +
                ".mode'='earliest-offset','connector.type'='kafka') */");
    }

    /**
     * with语法
     *
     * @param tableEnvironment
     */
    private static void with(StreamTableEnvironment tableEnvironment) {
        tableEnvironment.executeSql(expample_sql);
        tableEnvironment.executeSql("with test_with as(select * from test)select * from test_with")
                .print();
    }

    private static void window(StreamTableEnvironment tableEnvironment) {
        tableEnvironment.executeSql(expample_sql);
        tableEnvironment.executeSql("desc test").print();
        String tumbleWindowSQLGroupBy = "select window_start, window_end,sum(id) as count1 from TABLE(TUMBLE" +
                "(TABLE test," +
                "DESCRIPTOR (event_time),interval '5' second)) GROUP BY window_start, window_end";
        String hopWindowSQLGroupBy = "select window_start, window_end,sum(id) as count1 from TABLE(HOP(TABLE " +
                "test," +
                "DESCRIPTOR (event_time),INTERVAL '1' second,interval '5' second)) GROUP BY window_start, " +
                "window_end";
        String cumulateWindowSQLGroupBy = "select window_start, window_end,max(id) as count1 from TABLE(CUMULATE" +
                "(TABLE " +
                "test,DESCRIPTOR (event_time),INTERVAL '1' second,interval '5' second)) GROUP BY window_start, " +
                "window_end";
        tableEnvironment.executeSql(cumulateWindowSQLGroupBy)
                .print();
    }

    private static void windowAgg(StreamTableEnvironment tableEnvironment) {
        tableEnvironment.executeSql(expample_sql);
        String tumbleWindowSQLGroupingSets = "select window_start, window_end,id from TABLE" +
                "(TUMBLE(TABLE test,DESCRIPTOR (event_time),interval '5' second)) GROUP BY window_start, " +
                "window_end,grouping sets((id))";
        // rollup会携带全部的pv指标
        String rollUpSql = "select window_start,window_end,id,count(name) as pv from table" +
                "(tumble(table" +
                " " +
                "test," +
                "descriptor" +
                "(event_time)," +
                "interval '5' second)) group by window_start,window_end,rollup(id)";

        String cubeSql = "select window_start,window_end,window_time,id,sum(price) as sum_price from table" +
                "(tumble(table" +
                " " +
                "test," +
                "descriptor" +
                "(event_time)," +
                "interval '5' second)) group by window_start,window_end,window_time,cube(id)";



        tableEnvironment.executeSql(cubeSql).print();

    }
}
