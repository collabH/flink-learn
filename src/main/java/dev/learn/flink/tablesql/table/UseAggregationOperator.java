package dev.learn.flink.tablesql.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @fileName: UseAggregationOperatior.java
 * @description: UseAggregationOperatior.java类说明
 * @author: by echo huang
 * @date: 2020/9/3 11:49 下午
 */
public class UseAggregationOperator {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamEnvironment.getEnv(env);

        DataStreamSource<Tuple2<Integer, Integer>> datasource = env.fromElements(Tuple2.of(1, 10),
                Tuple2.of(1, 20), Tuple2.of(2, 3), Tuple2.of(2, 10));

        tableEnv.createTemporaryView("User", datasource, $("id"), $("name"));

        Table user = tableEnv.from("User");

        // groupBy
        tableEnv.fromDataStream(tableEnv.toRetractStream(user, Row.class), $("dml"), $("value"))
                .dropColumns($("dml"))
                .groupBy($("value").get(0))
                .select($("value").get(0).as("id"), $("value").get(1).count().as("count"))

                .execute().print();
    }
}
