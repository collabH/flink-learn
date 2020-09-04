package dev.learn.flink.tablesql.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.VARCHAR;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.localTime;
import static org.apache.flink.table.api.Expressions.row;

/**
 * @fileName: UseJoinOperator.java
 * @description: use join operator
 * @author: by echo huang
 * @date: 2020/9/4 3:48 下午
 */
public class UseJoinOperator {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        TableEnvironment tableEnv = StreamEnvironment.getBatchEnv();

        Table table = tableEnv.fromValues(ROW(FIELD("id", INT().notNull()), FIELD("name", VARCHAR(3).notNull()),
                FIELD("time1", TIMESTAMP(3).notNull())),
                row(1, "hsm", localTime()), row(2, "zzl", localTime()));
        Table table1 = tableEnv.fromValues(ROW(FIELD("aid", INT().notNull()), FIELD("user_id", INT().notNull()), FIELD("ext", VARCHAR(32)),
                FIELD("time", TIMESTAMP(3).notNull())),
                row(1, 1, "dxd", localTime()), row(2, 1, "dsd", localTime()),
                row(3, 2, "dd", localTime()), row(4, 3, "dsd", localTime()));

        // join
        table.join(table1, $("id").isEqual($("user_id")));

        // left out join
        table.leftOuterJoin(table1, $("id").isEqual($("user_id")));

        // right out join
        table.rightOuterJoin(table1, $("id").isEqual($("user_id")));

    }
}
