package dev.learn.flink.tablesql.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.concat;

/**
 * @fileName: UseColumnOperations.java
 * @description: UseColumnOperations.java类说明
 * @author: by echo huang
 * @date: 2020/9/3 11:04 下午
 */
public class UseColumnOperations {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamEnvironment.getEnv(env);

        DataStreamSource<Tuple2<Integer, String>> datasource = env.fromElements(Tuple2.of(1, "a"), Tuple2.of(2, "b"));

        tableEnv.fromDataStream(datasource, $("id"), $("name"))
                .addColumns($("name").as("name2"))
                .addOrReplaceColumns(concat($("id"), 3).as("id1"))
                .dropColumns($("name2"))
                .renameColumns($("name").as("name1"))
                .execute()
                .print();

    }
}
