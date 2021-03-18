package dev.learn.flink.tablesql.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import java.sql.Timestamp;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.currentTime;
import static org.apache.flink.table.api.Expressions.row;

/**
 * @fileName: UserTimeHandler.java
 * @description: UserTimeHandler.java类说明
 * @author: by echo huang
 * @date: 2021/3/18 10:59 下午
 */
public class UserTimeHandler {
    public static void main(String[] args) {
        TableEnvironment batchEnv = StreamEnvironment.getBatchEnv();

        Table sourceTable = batchEnv.fromValues(DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT()),
                DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3))),
                row(1, new Timestamp(System.currentTimeMillis())),
                row(2,new Timestamp(System.currentTimeMillis())));

        // use proctime
        sourceTable.select($("id"), currentTime().proctime());

        // use rowtime
        sourceTable.select($("name").rowtime());
    }
}
