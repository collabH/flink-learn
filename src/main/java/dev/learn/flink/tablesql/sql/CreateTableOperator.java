package dev.learn.flink.tablesql.sql;

import dev.learn.flink.tablesql.table.StreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @fileName: CreateTableOperator.java
 * @description: create table operator
 * @author: by echo huang
 * @date: 2020/9/6 11:07 上午
 */
public class CreateTableOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamEnvironment.getEnv(env);

        //create
        String ddl = "create table event(" +
                "id string,\n" +
                "`timestamp` string,\n" +
                "temperature string,\n" +
                " PRIMARY KEY (id) NOT ENFORCED)with(" +
                "'connector'='jdbc',\n" +
                "'url'='jdbc:mysql://localhost:3306/ds1',\n" +
                "'username'='root',\n" +
                "'password'='root',\n" +
                "'table-name'='event')";
        tableEnv.executeSql(ddl);

        Table event = tableEnv.sqlQuery("select * from event");

        tableEnv.toAppendStream(event, Row.class).print();

        env.execute();
    }
}
