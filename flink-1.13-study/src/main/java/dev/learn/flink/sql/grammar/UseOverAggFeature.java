package dev.learn.flink.sql.grammar;

import dev.learn.flink.FlinkEnvUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @fileName: UseOverAggFeature.java
 * @description: UseOverAggFeature.java类说明
 * @author: huangshimin
 * @date: 2021/9/1 10:44 下午
 */
public class UseOverAggFeature {
    static String expample_sql = "create table test(name string,id int,age bigint,price int,event_time " +
            "timestamp(3)," +
            "watermark for event_time as event_time - INTERVAL '1' second)with" +
            "('connector'='datagen', 'fields.id.min'='1', 'fields.id.max'='10','fields.price.min'='1', " +
            "'fields.price.max'='10','fields.age.kind'='sequence'," +
            "'fields.age.start'='1','fields.age.end'='100')";

    public static void main(String[] args) {
        StreamExecutionEnvironment streamEnv = FlinkEnvUtils.getStreamEnv();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(streamEnv);
//        topN(tableEnvironment);
//        windowTopN(tableEnvironment);
        distinctStream(tableEnvironment);
    }

    private static void topN(StreamTableEnvironment tableEnvironment) {
        tableEnvironment.executeSql(expample_sql);
        String topNSql = "select * from(select *, row_number()over w as rn from test window w as(partition by id " +
                "order " +
                "by event_time desc)) where rn<=5";
        tableEnvironment.executeSql(topNSql).print();
    }

    private static void windowTopN(StreamTableEnvironment tableEnvironment) {
        tableEnvironment.executeSql(expample_sql);
        String topNSql = "select * from(select id,window_start,window_end,price, row_number()over w " +
                "as rn " +
                "from (select id,window_start,window_end,sum(price) as price from table(tumble(table test,DESCRIPTOR" +
                "(event_time), INTERVAL '10' second)) group by id,window_start,window_end ) " +
                "window w as(partition by id," +
                "window_start,window_end " +
                "order by price desc)) where rn<=5";
        tableEnvironment.executeSql(topNSql).print();
    }

    private static void distinctStream(StreamTableEnvironment tableEnvironment) {
        tableEnvironment.executeSql(expample_sql);
        String distinctSql = "select * from (select *,row_number()over w as rn from test window w as(partition by id" +
                " order by event_time desc)) where rn =1";
        tableEnvironment.executeSql(distinctSql).print();
    }
}
