package dev.learn.flink.tablesql.sql;

import dev.learn.flink.tablesql.table.StreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.concat;
import static org.apache.flink.table.api.Expressions.e;
import static org.apache.flink.table.api.Expressions.row;

/**
 * @fileName: CreateViewOpeartor.java
 * @description: CreateViewOpeartor.java类说明
 * @author: by echo huang
 * @date: 2021/1/12 11:25 下午
 */
public class CreateViewOpeartor {
    public static void main(String[] args) {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment env = StreamEnvironment.getEnv(executionEnvironment);

        Table table = env.fromValues(DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT().notNull()),
                DataTypes.FIELD("name", DataTypes.VARCHAR(64).notNull())), row(1, "hsm"), row(2, "zs"));

        env.executeSql("create view test_view(id,name) as select id,name from " + table);
        env.executeSql("select * from test_view").print();

        env.createTemporaryView("test",table);
        env.sqlQuery("select * from test")
                .addColumns(concat($("id"),"aa").as("hh"))
                .execute().print();
    }
}
