package dev.learn.flink.tablesql.function;

import dev.learn.flink.tablesql.table.StreamEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.withColumns;

/**
 * @fileName: UseSystemFunction.java
 * @description: 系统内置函数
 * @author: by echo huang
 * @date: 2020/9/6 2:32 下午
 */
public class UseSystemFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamEnvironment.getEnv(env);
        Table table = tableEnv.fromDataStream(env.fromElements(Tuple2.of(1, 1), Tuple2.of(1, 2),
                Tuple2.of(2, 1), Tuple2.of(2, 2)), $("id"), $("id2"));
        tableEnv.createTemporaryView("test", table);

        // comparison function
        Table equalsTable = table.select($("id").isEqual($("id2")));
        Table isNotEqualTable = table.select($("id").isNotEqual($("id2")));
        Table isGreaterTable = table.select($("id").isGreater($("id2")));
        Table isGreaterOrEqualTable = table.select($("id").isGreaterOrEqual($("id2")));
        Table isLessTable = table.select($("id").isLess($("id2")));
        Table isLessOrEqualTable = table.select($("id").isLessOrEqual($("id2")));
        Table inTable = table.select($("id").in($("id2")));
        Table notBetweenTable = table.select($("id").notBetween(1, 2));
        Table betweenTable = table.select($("id").between(1, 2));

        tableEnv.toRetractStream(equalsTable, Row.class)
                .print();
        table.select(withColumns(1, 2));


        env.execute();


    }
}
