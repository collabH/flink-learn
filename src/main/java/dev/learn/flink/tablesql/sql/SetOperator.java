package dev.learn.flink.tablesql.sql;

import dev.learn.flink.tablesql.table.StreamEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.row;

/**
 * @fileName: SetOperator.java
 * @description: SetOperator.java类说明
 * @author: by echo huang
 * @date: 2020/9/6 10:31 上午
 */
public class SetOperator {
    public static void main(String[] args) throws Exception {

        TableEnvironment batchEnv = StreamEnvironment.getBatchEnv();

        Table table = batchEnv.fromValues(ROW(FIELD("id", INT().notNull())), row(1), row(2), row(3));
        Table table1 = batchEnv.fromValues(ROW(FIELD("id", INT().notNull())), row(4), row(5), row(6));
        batchEnv.createTemporaryView("user1",table);
        batchEnv.createTemporaryView("user2",table1);

        // union
        table.union(table1)
                .orderBy($("id").desc())
                .execute()
                .print();
    }
}
