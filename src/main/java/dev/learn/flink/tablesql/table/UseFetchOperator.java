package dev.learn.flink.tablesql.table;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.row;

/**
 * @fileName: UseFetchOperator.java
 * @description: 仅支持batch模式
 * @author: by echo huang
 * @date: 2020/9/4 6:03 下午
 */
public class UseFetchOperator {
    public static void main(String[] args) {
        TableEnvironment batchEnv = StreamEnvironment.getBatchEnv();

        Table user = batchEnv.fromValues(ROW(FIELD("id", INT().notNull()), FIELD("name", STRING().nullable())),
                row(1, "hsm"), row(5, "hsm"), row(3, "fdsf"), row(2, "fdsf"));

        // order by
        user.orderBy($("id").desc())
                .execute()
                .print();

        // offset&fetch

        user.orderBy($("id").asc())
                .fetch(2)
                .execute()
                .print();

    }
}
