package dev.learn.flink.sql.grammar;

import dev.learn.flink.FlinkEnvUtils;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import javax.print.DocFlavor;

/**
 * @fileName: SQLGrammarFeature.java
 * @description: flink sql语法
 * @author: huangshimin
 * @date: 2021/8/29 7:02 下午
 */
public class SQLGrammarFeature {
    static String expample_sql = "create table test(name string,id int,age bigint,price int,event_time " +
            "timestamp(3)," +
            "watermark for event_time as event_time - INTERVAL '1' second)with" +
            "('connector'='datagen', 'fields.id.min'='1', 'fields.id.max'='10','fields.price.min'='1', " +
            "'fields.price.max'='10','fields.age.kind'='sequence'," +
            "'fields.age.start'='1','fields.age.end'='100')";

    public static void main(String[] args) {
        StreamExecutionEnvironment streamEnv = FlinkEnvUtils.getStreamEnv();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(streamEnv);
        TableEnvironment batchTableEnvironment = TableEnvironment.create(
                EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build());
//        with(tableEnvironment);
//        window(tableEnvironment);
//        windowAgg(tableEnvironment);
//        groupFunction(batchTableEnvironment);
        overAgg(tableEnvironment);
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


        String windowStartSql = "select tumble_start(event_time,INTERVAL '5' second) as wStart,sum(price) as " +
                "priceC from test group by tumble(event_time,INTERVAL '5' second)";

        tableEnvironment.executeSql(windowStartSql).print();
    }

    private static void groupFunction(TableEnvironment tableEnvironment) {
        String countSql = "select count(id) as pv from test";
        String distinctSql = "select distinct(id) from test";
        String groupingSetsSql = "SELECT supplier_id, rating, COUNT(*) AS total\n" +
                "FROM (VALUES\n" +
                "    ('supplier1', 'product1', 4),\n" +
                "    ('supplier1', 'product2', 3),\n" +
                "    ('supplier2', 'product3', 3),\n" +
                "    ('supplier2', 'product4', 4))\n" +
                "AS Products(supplier_id, product_id, rating)\n" +
                "GROUP BY GROUPING SETS ((supplier_id, rating), (supplier_id), ())";

        // 可以达到和grouping sets（(supplier_id, rating), (supplier_id), ()）的效果
        String rollupSql="SELECT supplier_id, rating, COUNT(*)\n" +
                "FROM (VALUES\n" +
                "    ('supplier1', 'product1', 4),\n" +
                "    ('supplier1', 'product2', 3),\n" +
                "    ('supplier2', 'product3', 3),\n" +
                "    ('supplier2', 'product4', 4))\n" +
                "AS Products(supplier_id, product_id, rating)\n" +
                "GROUP BY ROLLUP (supplier_id, rating)";

        String cubeSql="SELECT supplier_id, rating, product_id, COUNT(*)\n" +
                "FROM (VALUES\n" +
                "    ('supplier1', 'product1', 4),\n" +
                "    ('supplier1', 'product2', 3),\n" +
                "    ('supplier2', 'product3', 3),\n" +
                "    ('supplier2', 'product4', 4))\n" +
                "AS Products(supplier_id, product_id, rating)\n" +
                "GROUP BY CUBE (supplier_id, rating, product_id)";

        // cube三个字段等价于GROUPING SET
        String equalCubeGroupingSql2="SELECT supplier_id, rating, product_id, COUNT(*)\n" +
                "FROM (VALUES\n" +
                "    ('supplier1', 'product1', 4),\n" +
                "    ('supplier1', 'product2', 3),\n" +
                "    ('supplier2', 'product3', 3),\n" +
                "    ('supplier2', 'product4', 4))\n" +
                "AS Products(supplier_id, product_id, rating)\n" +
                "GROUP BY GROUPING SETS (\n" +
                "    ( supplier_id, product_id, rating ),\n" +
                "    ( supplier_id, product_id         ),\n" +
                "    ( supplier_id,             rating ),\n" +
                "    ( supplier_id                     ),\n" +
                "    (              product_id, rating ),\n" +
                "    (              product_id         ),\n" +
                "    (                          rating ),\n" +
                "    (                                 ))";
        tableEnvironment.executeSql(equalCubeGroupingSql2).print();
    }

    /**
     * SELECT
     *   agg_func(agg_col) OVER (
     *     [PARTITION BY col1[, col2, ...]]
     *     ORDER BY time_col
     *     range_definition),
     *   ...
     * FROM ...
     * @param tableEnvironment
     */
    private static void overAgg(StreamTableEnvironment tableEnvironment){
        tableEnvironment.executeSql(expample_sql);
        // 以下RANGE间隔定义了将所有time属性小于当前行最多5 s的行包含在聚合中。
        String overSql="select id,sum(price) over w as sum_price from test" +
                " WINDOW w AS ( PARTITION by id order by event_time RANGE BETWEEN INTERVAL '5' second PRECEDING AND " +
                "CURRENT ROW)";
        tableEnvironment.executeSql(overSql).print();
    }

}
