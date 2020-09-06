package dev.learn.flink.tablesql.sql;

import dev.learn.flink.tablesql.table.StreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

/**
 * @fileName: CreateCatalogOperator.java
 * @description: CreateCatalogOperator.java类说明
 * @author: by echo huang
 * @date: 2020/9/6 1:45 下午
 */
public class CreateCatalogOperator {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamEnvironment.getEnv(env);

        tableEnv.executeSql("create catalog test");

        System.out.println(Arrays.toString(tableEnv.listCatalogs()));
    }
}
