package dev.learn.flink.tablesql.table.udf;

import org.apache.flink.table.functions.TableFunction;

/**
 * @fileName: SplitFunction.java
 * @description: SplitFunction.java类说明
 * @author: by echo huang
 * @date: 2020/9/4 10:20 上午
 */
public class SplitFunction extends TableFunction<String> {
    public Integer eval(String value, int index) {
        return Integer.valueOf(value.split(",")[index]);
    }
}
