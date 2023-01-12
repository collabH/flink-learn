package dev.learn.flink.sql.table;

import dev.learn.flink.FlinkEnvUtils;
import dev.learn.flink.sql.table.udaf.MyAggFunc;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.row;

/**
 * @fileName: TableApiDemo1.java
 * @description: table api demo
 * @author: huangshimin
 * @date: 2023/1/12 8:04 PM
 */
public class TableApiDemo1 {
    public static void main(String[] args) {
        StreamExecutionEnvironment streamEnv = FlinkEnvUtils.getStreamEnv();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(streamEnv);
        Table source = streamTableEnvironment.fromValues(
                row(1, "hsm", 100),
                row(1, "ls", 90),
                row(2, "wy", 99),
                row(3, "zs", 4),
                row(2, "zs1", 30),
                row(4, "ls1", 12),
                row(2, "ls2", 94),
                row(1, "ls4", 91),
                row(2, "ls5", 80),
                row(4, "ls12", 70),
                row(3, "ls6", 60)
        ).as("id", "hsm", "score");

        streamTableEnvironment.createTemporaryFunction("minMaxScore", new MyAggFunc());

        // use group agg
        source.groupBy($("id"))
                .aggregate(call("minMaxScore", $("score")).as("max", "min"))
                .select($("id"), $("max"), $("min"))
                .execute().print();
    }


}
